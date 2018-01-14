package com.github.yarosla.httpstorage;

import ch.qos.logback.classic.Level;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.ipc.netty.http.server.HttpServer;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.springframework.web.reactive.function.server.RequestPredicates.*;
import static org.springframework.web.reactive.function.server.RouterFunctions.*;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        Parameters parameters = new Parameters();
        JCommander jCommander = JCommander.newBuilder()
                .addObject(parameters)
                .build();
        jCommander.parse(args);

        if (parameters.isHelp()) {
            jCommander.usage();
            return;
        }

        if (parameters.isDebug()) {
            ch.qos.logback.classic.Logger packageLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.github.yarosla.httpstorage");
            packageLogger.setLevel(Level.DEBUG);
        }

        HttpServer.create(parameters.getHost(), parameters.getPort())
                .newHandler(new ReactorHttpHandlerAdapter(toHttpHandler(buildRouter(parameters), handlerStrategies(parameters))))
                .block();

        logger.info("HTTP store is listening on http://{}:{}/", parameters.getHost(), parameters.getPort());

        Thread.currentThread().join(); // block indefinitely
    }

    private static HandlerStrategies handlerStrategies(Parameters parameters) {
        if (parameters.isCorsEnabled()) {
            CorsConfiguration corsConfiguration = new CorsConfiguration();
            corsConfiguration.addAllowedOrigin(CorsConfiguration.ALL);
            corsConfiguration.setAllowedMethods(Arrays.asList(
                    HttpMethod.GET.name(), HttpMethod.POST.name(),
                    HttpMethod.PUT.name(), HttpMethod.DELETE.name()));
            corsConfiguration.addAllowedHeader(CorsConfiguration.ALL);
            corsConfiguration.setExposedHeaders(Arrays.asList(HttpHeaders.ETAG, HttpHeaders.LOCATION));
            corsConfiguration.setAllowCredentials(true);
            corsConfiguration.setMaxAge(TimeUnit.HOURS.toSeconds(1));
            return HandlerStrategies.builder().webFilter(new CorsWebFilter(request -> corsConfiguration)).build();
        } else {
            return HandlerStrategies.withDefaults();
        }
    }

    private static RouterFunction<ServerResponse> buildRouter(Parameters parameters) {
        Store store = new Store(parameters.getMemoryLimit());

        String collectionPattern = "/v1/{" + Store.COLLECTION_VAR_NAME + "}";
        String documentPattern = collectionPattern + "/{" + Store.DOCUMENT_VAR_NAME + "}";
        RouterFunction<ServerResponse> routerFunction = route(GET(documentPattern), store::retrieveDocument)
                .andRoute(PUT(documentPattern), store::updateDocument)
                .andRoute(POST(collectionPattern), store::createDocument)
                .andRoute(GET(collectionPattern), store::listDocuments)
                .andRoute(DELETE(documentPattern), store::deleteDocument)
                .andRoute(GET("/stats"), store::statistics);
        //noinspection ConstantConditions
        if (parameters.getStaticResourcesPath() != null) {
            routerFunction = routerFunction.and(resources("/**", new FileSystemResource(parameters.getStaticResourcesPath() + "/")));
        }
        return routerFunction;
    }
}
