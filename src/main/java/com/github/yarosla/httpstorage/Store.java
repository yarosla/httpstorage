package com.github.yarosla.httpstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class Store {
    private final static Logger logger = LoggerFactory.getLogger(Store.class);

    static final String COLLECTION_VAR_NAME = "collection";
    static final String DOCUMENT_VAR_NAME = "id";

    private static final CacheControl CACHE_CONTROL = CacheControl.empty().cachePrivate().mustRevalidate();

    private final Cache<String, Document> cache;

    private final DirectProcessor<String> updates = DirectProcessor.create();
    private final FluxSink<String> updatesSink = updates.sink();

    Store(long memoryLimit) {
        logger.info("Creating LRU cache limited to {} bytes of content", memoryLimit);
        cache = Caffeine.newBuilder()
                .weigher((String key, Document document) -> document.content.length)
                .maximumWeight(memoryLimit)
                .recordStats()
                .build();
    }

    Mono<ServerResponse> createDocument(ServerRequest request) {
        String collectionId = collectionId(request);
        MediaType contentType = requestMediaType(request);
        long timestamp = System.currentTimeMillis();
        return request.bodyToMono(ByteArrayResource.class)
                .map(ByteArrayResource::getByteArray)
                .flatMap(bytes -> {
                    Document document = new Document(contentType, bytes, collectionId, 0, timestamp);
                    String documentId = storeNewDocument(collectionId, document);
                    logger.debug("createDocument {} <- {} bytes", documentId, bytes.length);
                    return documentResponse(ServerResponse.created(request.uriBuilder().pathSegment(documentId).build(collectionId)), document);
                });
    }

    private String storeNewDocument(String collectionId, Document document) {
        String key;
        ConcurrentMap<String, Document> concurrentMap = cache.asMap();
        do {
            key = key(collectionId, Integer.toHexString(ThreadLocalRandom.current().nextInt()));
        } while (concurrentMap.putIfAbsent(key, document) != null);
        updatesSink.next(key);
        return key;
    }

    Mono<ServerResponse> retrieveDocument(ServerRequest request) {
        String documentId = documentId(request);
        String key = key(collectionId(request), documentId);
        List<String> conditions = request.headers().header("If-None-Match");
        int timeout = request.headers().header("Timeout").stream().findAny().map(Integer::parseInt).orElse(0);
        Document originalDocument = cache.getIfPresent(key);

        if (originalDocument == null) {
            logger.debug("retrieveDocument {} -> not found", documentId);
            return ServerResponse.notFound().build();
        } else if (!conditions.contains("\"" + originalDocument.version + "\"")) {
            logger.debug("retrieveDocument {} -> version {}: {} bytes", documentId, originalDocument.version, originalDocument.content.length);
            return documentResponse(ServerResponse.ok(), originalDocument);
        } else if (timeout <= 0) {
            logger.debug("retrieveDocument {} -> version {}: not modified", documentId, originalDocument.version);
            return documentNotModifiedResponse(originalDocument);
        } else {
            logger.debug("retrieveDocument {} -> version {}: waiting for newer version up to {} millis", documentId, originalDocument.version, timeout);
            return updates
                    .filter(k -> k.equals(key))
                    .mergeWith(Mono.just(key)) // re-check version after subscribing to updates
                    .flatMap(k -> {
                        Document doc = cache.getIfPresent(k);
                        return doc != null ? Mono.just(doc) : Mono.error(new NoDocumentException());
                    })
                    .filter(doc -> !conditions.contains("\"" + doc.version + "\""))
                    .take(1).single()
                    .flatMap(doc -> {
                        logger.debug("retrieveDocument {} -> newer version {}: {} bytes", documentId, doc.version, doc.content.length);
                        return documentResponse(ServerResponse.ok(), doc);
                    })
                    .timeout(Duration.ofMillis(timeout))
                    .onErrorResume(TimeoutException.class, e -> {
                        logger.debug("retrieveDocument {} -> version {}: no newer version within {} millis", documentId, originalDocument.version, timeout);
                        return documentNotModifiedResponse(originalDocument);
                    })
                    .onErrorResume(NoDocumentException.class, e -> {
                        logger.debug("retrieveDocument {} -> version {}: document deleted", documentId, originalDocument.version);
                        return ServerResponse.notFound().build();
                    });
        }
    }

    Mono<ServerResponse> updateDocument(ServerRequest request) {
        String collectionId = collectionId(request);
        String documentId = documentId(request);
        String key = key(collectionId, documentId);
        MediaType contentType = requestMediaType(request);
        List<String> preconditions = request.headers().header("If-Match");
        long timestamp = System.currentTimeMillis();
        return request.bodyToMono(ByteArrayResource.class)
                .map(ByteArrayResource::getByteArray)
                .flatMap(bytes -> {
                    ConcurrentMap<String, Document> concurrentMap = cache.asMap();
                    Document existingDocument = concurrentMap.get(key);
                    if (existingDocument == null) {
                        logger.debug("updateDocument {} <- {} bytes; document not found", documentId, bytes.length);
                        return ServerResponse.notFound().build();
                    } else if (preconditions.isEmpty()) {
                        logger.debug("updateDocument {} <- {} bytes; precondition required {}", documentId, bytes.length, existingDocument.version);
                        return documentResponse(ServerResponse.status(HttpStatus.PRECONDITION_REQUIRED), existingDocument);
                    } else if (!preconditions.contains("\"" + existingDocument.version + "\"")) {
                        logger.debug("updateDocument {} <- {} bytes; precondition failed: {} not in {}", documentId, bytes.length, existingDocument.version, preconditions);
                        return documentResponse(ServerResponse.status(HttpStatus.PRECONDITION_FAILED), existingDocument);
                    } else {
                        Document updatedDocument = new Document(contentType, bytes, collectionId, existingDocument.version + 1, timestamp);
                        if (concurrentMap.replace(key, existingDocument, updatedDocument)) {
                            logger.debug("updateDocument {} <- {} bytes", documentId, bytes.length);
                            updatesSink.next(key);
                            return documentResponse(ServerResponse.ok(), updatedDocument);
                        } else {
                            logger.debug("updateDocument {} <- {} bytes; precondition failed: document has been replaced by other thread", documentId, bytes.length);
                            return documentResponse(ServerResponse.status(HttpStatus.PRECONDITION_FAILED), concurrentMap.get(key));
                        }
                    }
                });
    }

    private String key(@SuppressWarnings("unused") String collectionId, String documentId) {
        return documentId; // collectionId + "/" + documentId
    }

    private Mono<ServerResponse> documentResponse(ServerResponse.BodyBuilder responseBuilder, Document document) {
        return responseBuilder
                .contentType(document.contentType)
                .contentLength(document.content.length)
                .eTag(Long.toString(document.version))
                .lastModified(lastModified(document))
                .header(HttpHeaders.EXPIRES, "0")
                .cacheControl(CACHE_CONTROL)
                .body(BodyInserters.fromObject(document.content));
    }

    private Mono<ServerResponse> documentNotModifiedResponse(Document document) {
        return ServerResponse.status(HttpStatus.NOT_MODIFIED)
                .eTag(Long.toString(document.version))
                .lastModified(lastModified(document))
                .cacheControl(CACHE_CONTROL)
                .build();
    }

    Mono<ServerResponse> listDocuments(ServerRequest request) {
        String collectionId = collectionId(request);
        logger.debug("listDocuments {}", collectionId);
        List<DocumentInfo> documentInfoList = cache.asMap().entrySet().stream()
                .filter(entry -> collectionId.equals(entry.getValue().collectionId))
                .map(entry -> new DocumentInfo(entry.getKey(), entry.getValue().version, entry.getValue().timestamp))
                .collect(Collectors.toList());
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .cacheControl(CacheControl.noCache())
                .body(BodyInserters.fromObject(documentInfoList));
    }

    Mono<ServerResponse> deleteDocument(ServerRequest request) {
        String documentId = documentId(request);
        String key = key(collectionId(request), documentId);
        logger.debug("deleteDocument {}", documentId);
        cache.invalidate(key);
        updatesSink.next(key);
        return ServerResponse.noContent().build();
    }

    Mono<ServerResponse> statistics(@SuppressWarnings("unused") ServerRequest request) {
        logger.debug("collecting statistics");
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .cacheControl(CacheControl.noCache())
                .body(BodyInserters.fromObject(new CacheStatistics(cache)));
    }

    private String collectionId(ServerRequest request) {
        return request.pathVariable(COLLECTION_VAR_NAME);
    }

    private String documentId(ServerRequest request) {
        return request.pathVariable(DOCUMENT_VAR_NAME);
    }

    private ZonedDateTime lastModified(Document document) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(document.timestamp), ZoneOffset.UTC);
    }

    private MediaType requestMediaType(ServerRequest request) {
        return request.headers().contentType().orElse(MediaType.APPLICATION_OCTET_STREAM);
    }

    private static class Document {
        private MediaType contentType;
        private byte[] content;
        @SuppressWarnings("unused")
        private String collectionId;
        private long version;
        private long timestamp;

        Document(MediaType contentType, byte[] content, String collectionId, long version, long timestamp) {
            this.contentType = contentType;
            this.content = content;
            this.collectionId = collectionId;
            this.version = version;
            this.timestamp = timestamp;
        }
    }

    @SuppressWarnings("unused")
    static class DocumentInfo {
        private String id;
        private long version;
        private long lastModified;

        @JsonCreator
        DocumentInfo(@JsonProperty("id") String id, @JsonProperty("version") long version, @JsonProperty("lastModified") long lastModified) {
            this.id = id;
            this.version = version;
            this.lastModified = lastModified;
        }

        public String getId() {
            return id;
        }

        public long getVersion() {
            return version;
        }

        public long getLastModified() {
            return lastModified;
        }
    }

    private static class NoDocumentException extends RuntimeException {
    }
}
