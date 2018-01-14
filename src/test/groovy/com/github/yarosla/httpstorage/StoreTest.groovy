package com.github.yarosla.httpstorage

import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.FluxExchangeResult
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.SocketUtils
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import spock.lang.Specification
import spock.lang.Stepwise

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Function

@Stepwise
@Slf4j
class StoreTest extends Specification {

    static int port
    static mainThread = new Thread('serverMain')
    static WebTestClient client
    static String documentLocation

    def setupSpec() {
        port = SocketUtils.findAvailableTcpPort()
        mainThread.start {
            Main.main('-P', Integer.toString(port))
        }
        client = WebTestClient.bindToServer().baseUrl("http://127.0.0.1:$port/").build()
    }

    def "original stats are zero"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/stats').exchange()
        then:
        responseSpec
                .expectStatus().isOk()
                .expectBody().json('{"currentCount":0,"currentWeight":0,"evictionCount":0,"evictionWeight":0,"hitCount":0,"missCount":0,"requestCount":0,"hottest":[]}')
    }

    def "request non-existent document"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/v1/coll1/123').exchange()
        then:
        responseSpec.expectStatus().isNotFound()
    }

    def "request empty document list"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/v1/coll1').exchange()
        then:
        responseSpec.expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<List<Store.DocumentInfo>>() {})
                .isEqualTo([])
    }

    def "create document"() {
        when:
        FluxExchangeResult<String> result = client.post().uri('/v1/coll1')
                .contentType(MediaType.TEXT_PLAIN)
                .syncBody('Hello!')
                .exchange()
                .returnResult(String)
        HttpHeaders headers = result.responseHeaders
        log.info 'headers: {}', headers
        then:
        result.status == HttpStatus.CREATED
        headers.containsKey('Location')
        headers.get('ETag') == ['"0"']
        headers.containsKey('Last-Modified')
        result.responseBodyContent == 'Hello!'.bytes
        when:
        documentLocation = headers.get('Location')[0]
        then:
        documentLocation.matches("http://127\\.0\\.0\\.1:$port/v1/coll1/[0-9a-f]+")
    }

    def "fetch document"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri(documentLocation)
                .exchange()
        then:
        responseSpec
                .expectStatus().isOk()
                .expectHeader().valueEquals('ETag', '"0"')
                .expectHeader().contentType(MediaType.TEXT_PLAIN)
                .expectBody(String).isEqualTo('Hello!')
    }

    def "fetch document with condition"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri(documentLocation)
                .header('if-none-match', '"0"')
                .exchange()
        then:
        responseSpec
                .expectStatus().isNotModified()
                .expectHeader().valueEquals('ETag', '"0"')
                .expectBody().isEmpty()
    }

    def "request single document list"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/v1/coll1').exchange()
        def result = responseSpec.expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<List<Store.DocumentInfo>>() {})
                .returnResult().responseBody
        result.each {
            log.info('got doc info {} {} {}', it.id, it.version, it.lastModified)
        }
        then:
        result.size() == 1
        result[0].version == 0
        documentLocation.endsWith('/' + result[0].id)
    }

    def "update document"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.put().uri(documentLocation)
                .contentType(MediaType.TEXT_PLAIN)
                .header('if-match', '"0"')
                .syncBody('Bye...')
                .exchange()
        then:
        responseSpec
                .expectStatus().isOk()
                .expectHeader().valueEquals('ETag', '"1"')
                .expectHeader().contentType(MediaType.TEXT_PLAIN)
                .expectBody(String).isEqualTo('Bye...')
    }

    def "try update without precondition"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.put().uri(documentLocation)
                .contentType(MediaType.TEXT_PLAIN)
                .syncBody('Update again')
                .exchange()
        then:
        responseSpec
                .expectStatus().isEqualTo(HttpStatus.PRECONDITION_REQUIRED)
                .expectHeader().valueEquals('ETag', '"1"')
                .expectHeader().contentType(MediaType.TEXT_PLAIN)
                .expectBody(String).isEqualTo('Bye...')
    }

    def "try update with wrong precondition"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.put().uri(documentLocation)
                .contentType(MediaType.TEXT_PLAIN)
                .header('if-match', '"0"')
                .syncBody('Update again')
                .exchange()
        then:
        responseSpec
                .expectStatus().isEqualTo(HttpStatus.PRECONDITION_FAILED)
                .expectHeader().valueEquals('ETag', '"1"')
                .expectHeader().contentType(MediaType.TEXT_PLAIN)
                .expectBody(String).isEqualTo('Bye...')
    }

    def "updated document list"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/v1/coll1').exchange()
        def result = responseSpec.expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<List<Store.DocumentInfo>>() {})
                .returnResult().responseBody
        result.each {
            log.info('got doc info {} {} {}', it.id, it.version, it.lastModified)
        }
        then:
        result.size() == 1
        result[0].version == 1
        documentLocation.endsWith('/' + result[0].id)
    }

    def "stats"() {
        def documentId = documentLocation.substring(documentLocation.lastIndexOf('/') + 1)
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/stats').exchange()
        then:
        responseSpec
                .expectStatus().isOk()
                .expectBody().json('{"currentCount":1,"currentWeight":6,"evictionCount":0,"evictionWeight":0,"hitCount":2,"missCount":1,"requestCount":3,"hottest":["' + documentId + '"]}')
    }

    def "poll for newer version - not modified"() {
        long t1 = System.currentTimeMillis()
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri(documentLocation)
                .header('if-none-match', '"1"')
                .header('timeout', '2000')
                .exchange()
        long t2 = System.currentTimeMillis()
        then:
        responseSpec
                .expectStatus().isNotModified()
                .expectHeader().valueEquals('ETag', '"1"')
        1900 < t2 - t1
        t2 - t1 < 2500
    }

    def "poll for newer version - got one"() {
        when:
        Thread.start {
            TimeUnit.MILLISECONDS.sleep(1000)
            log.info 'updating the document...'
            client.put().uri(documentLocation)
                    .contentType(MediaType.TEXT_PLAIN)
                    .header('if-match', '"1"')
                    .syncBody('Not yet')
                    .exchange()
                    .expectStatus().isOk()
            log.info 'updating the document - done'
        }
        log.info 'poll document...'
        long t1 = System.currentTimeMillis()
        WebTestClient.ResponseSpec responseSpec = client.get().uri(documentLocation)
                .header('if-none-match', '"1"')
                .header('timeout', '2000')
                .exchange()
        log.info 'poll document - complete'
        long t2 = System.currentTimeMillis()
        then:
        responseSpec
                .expectStatus().isOk()
                .expectHeader().valueEquals('ETag', '"2"')
                .expectHeader().contentType(MediaType.TEXT_PLAIN)
                .expectBody(String).isEqualTo('Not yet')
        900 < t2 - t1
        t2 - t1 < 1500
    }

    def "poll for newer version - document deleted"() {
        when:
        Thread.start {
            TimeUnit.MILLISECONDS.sleep(1000)
            log.info 'updating the document...'
            client.delete().uri(documentLocation)
                    .exchange()
                    .expectStatus().isOk()
            log.info 'updating the document - done'
        }
        log.info 'poll document...'
        long t1 = System.currentTimeMillis()
        WebTestClient.ResponseSpec responseSpec = client.get().uri(documentLocation)
                .header('if-none-match', '"2"')
                .header('timeout', '2000')
                .exchange()
        log.info 'poll document - complete'
        long t2 = System.currentTimeMillis()
        then:
        responseSpec
                .expectStatus().isNotFound()
        900 < t2 - t1
        t2 - t1 < 1500
    }

    def "delete document"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.delete().uri(documentLocation)
                .exchange()
        then:
        responseSpec
                .expectStatus().isEqualTo(HttpStatus.NO_CONTENT)
    }

    def "document list is empty again"() {
        when:
        WebTestClient.ResponseSpec responseSpec = client.get().uri('/v1/coll1').exchange()
        then:
        responseSpec.expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<List<Store.DocumentInfo>>() {})
                .isEqualTo([])
    }

    def "updates from two clients"() {
        setup:
        StoreClient<State> sc = new StoreClient<>(State, "http://127.0.0.1:$port/v1/concurrent", new State())
        StoreClient<State> sc2 = new StoreClient<>(State, sc.uri)
        def incrementer = { new State(count: it.count + 1) }
        when:
        sc.update(incrementer)
        then:
        sc.state.count == 1
        when:
        sc.update(incrementer)
        then:
        sc.state.count == 2
        when:
        sc2.update(incrementer)
        then:
        sc2.state.count == 3
    }

    def "many concurrent clients"() {
        setup:
        StoreClient<State> sc = new StoreClient(State, "http://127.0.0.1:$port/v1/concurrent", new State())
        def incrementer = { new State(count: it.count + 1) }
        def numClients = 300
        when:
        Flux.range(1, numClients)
                .flatMap(
                {
                    Mono.just(1)
                            .subscribeOn(Schedulers.fromExecutor(Executors.newFixedThreadPool(numClients)))
                            .doOnNext({ new StoreClient<State>(State, sc.uri).update(incrementer) })
                })
                .blockLast()
        sc.update(incrementer)
        then:
        sc.state.count == numClients + 1
    }

    void cleanupSpec() {
        log.info 'shutting down server'
        mainThread.interrupt()
    }

    class StoreClient<T> {
        private WebClient webClient
        private String uri
        private long version
        private T state
        private Class<T> cls

        StoreClient(Class<T> cls, String documentUri) {
            this.cls = cls
            webClient = WebClient.builder()
                    .baseUrl(documentUri)
                    .build()
            uri = documentUri
            state = Objects.requireNonNull(webClient.get()
                    .exchange()
                    .flatMap(
                    { response ->
                        version = response.headers().header('ETag').stream().findFirst().map({
                            Long.parseLong(it.substring(1, it.length() - 1))
                        }).get()
                        log.info 'fetched {} version {} of {} bytes', response.statusCode(), version, response.headers().contentLength()
                        response.bodyToMono(cls)
                    })
                    .block(), 'fetch failed')
        }

        StoreClient(Class<T> cls, String collectionUri, T initialState) {
            this.cls = cls
            webClient = WebClient.builder()
                    .baseUrl(collectionUri)
                    .build()
            webClient.post()
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .body(Mono.just(initialState), cls)
                    .exchange()
                    .flatMap(
                    { response ->
                        version = response.headers().header('ETag').stream().findFirst().map({
                            Long.parseLong(it.substring(1, it.length() - 1))
                        }).get()
                        uri = response.headers().header('Location')[0]
                        log.info 'created document {} of version {}', uri, version
                        response.bodyToMono(cls)
                    })
                    .doOnNext(
                    {
                        state = Objects.requireNonNull(it, 'failed to load just created document')
                    })
                    .block()
        }

        String getUri() {
            return uri
        }

        T getState() {
            return state
        }

        def update(Function<T, T> updater) {
            log.info 'updating...'
            Mono.just(1)
                    .flatMap(
                    {
                        webClient.put()
                                .uri(uri)
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .header('If-Match', '"' + version + '"')
                                .body(
                                Mono.just(state)
                                        .map(updater)
                                        .doOnSuccess(
                                        {
                                            log.info('sending version {} with state {}', version, it)
                                        }), cls)
                                .exchange()
                                .flatMap(
                                { response ->
                                    long expectedVersion = version
                                    version = response.headers().header('ETag').stream().findFirst().map({
                                        Long.parseLong(it.substring(1, it.length() - 1))
                                    }).get()
                                    log.info 'got response {} with version {}', response.statusCode(), version
                                    response.bodyToMono(cls)
                                            .doOnSuccess(
                                            {
                                                state = Objects.requireNonNull(it, 'failed to load state after update')
                                                log.info 'got response {} with state {}', response.statusCode(), state
                                                if (response.statusCode() == HttpStatus.PRECONDITION_FAILED) {
                                                    log.warn('precondition failed; expected={} actual={}; retrying', expectedVersion, version)
                                                    throw new RuntimeException('precondition failed - force retry')
                                                }
                                            })
                                })
                    })
                    .retry()
                    .block()
        }
    }

    @ToString(includePackage = false)
    static class State {
        int count = 0
    }
}
