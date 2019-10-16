/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.api;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FrontierApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierApiServer.class);

    private final Server server;
    private final ExecutorService threadPool;
    private long shutdownTimeoutMillis = 60 * 1000;

    public FrontierApiServer(int port, int shutdownTimeoutSeconds, Frontier frontier) {
        this(ServerBuilder.forPort(port), frontier);
        this.shutdownTimeoutMillis = shutdownTimeoutSeconds * 1000;
    }

    public FrontierApiServer(ServerBuilder<?> serverBuilder, Frontier frontier) {
        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        threadPool = Executors.newCachedThreadPool();
        serverBuilder.executor(threadPool);

        FrontierService frontierService = new FrontierService(frontier);
        server = serverBuilder.addService(tracingInterceptor.intercept(frontierService)).build();
    }

    public FrontierApiServer start() {
        try {
            server.start();

            LOG.info("Controller api listening on {}", server.getPort());

            return this;
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        long startTime = System.currentTimeMillis();
        server.shutdown();
        try {
            long timeLeftBeforeKill = shutdownTimeoutMillis - (System.currentTimeMillis() - startTime);
            server.awaitTermination(timeLeftBeforeKill, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            server.shutdownNow();
        }
        threadPool.shutdown();
        try {
            long timeLeftBeforeKill = shutdownTimeoutMillis - (System.currentTimeMillis() - startTime);
            threadPool.awaitTermination(timeLeftBeforeKill, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
        System.err.println("*** server shut down");
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() {
        if (server != null) {
            try {
                server.awaitTermination();
            } catch (InterruptedException ex) {
                close();
                throw new RuntimeException(ex);
            }
        }
    }

}
