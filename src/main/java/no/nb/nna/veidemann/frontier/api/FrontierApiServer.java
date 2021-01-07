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

import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.services.HealthStatusManager;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FrontierApiServer {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierApiServer.class);

    private final Server server;
    private final ScheduledExecutorService healthCheckerExecutorService;
    private long shutdownTimeoutMillis = 60 * 1000;
    final FrontierService frontierService;
    final HealthStatusManager health;

    public FrontierApiServer(int port, int shutdownTimeoutSeconds, Frontier frontier) {
        this(ServerBuilder.forPort(port), frontier);
        this.shutdownTimeoutMillis = shutdownTimeoutSeconds * 1000;
    }

    public FrontierApiServer(ServerBuilder<?> serverBuilder, Frontier frontier) {
        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        healthCheckerExecutorService = Executors.newScheduledThreadPool(1);
        health = new HealthStatusManager();
        healthCheckerExecutorService.scheduleAtFixedRate(new HealthChecker(frontier, health), 0, 1, TimeUnit.SECONDS);

        frontierService = new FrontierService(frontier);
        server = serverBuilder
                .addService(ServerInterceptors.intercept(tracingInterceptor.intercept(frontierService),
                        ConcurrencyLimitServerInterceptor.newBuilder(
                                new GrpcServerLimiterBuilder()
                                        .partitionByMethod()
                                        .partition(FrontierGrpc.getCrawlSeedMethod().getFullMethodName(), 0.9)
                                        .partition(FrontierGrpc.getGetNextPageMethod().getFullMethodName(), 0.02)
                                        .partition(FrontierGrpc.getBusyCrawlHostGroupCountMethod().getFullMethodName(), 0.02)
                                        .partition(FrontierGrpc.getQueueCountForCrawlExecutionMethod().getFullMethodName(), 0.02)
                                        .partition(FrontierGrpc.getQueueCountForCrawlHostGroupMethod().getFullMethodName(), 0.02)
                                        .partition(FrontierGrpc.getQueueCountTotalMethod().getFullMethodName(), 0.02)
                                        .limit(WindowedLimit.newBuilder()
                                                .build(Gradient2Limit.newBuilder()
                                                        .build()))
                                        .build())
                                .build()))
                .addService(health.getHealthService())
                .build();
    }

    public FrontierApiServer start() {
        try {
            server.start();

            LOG.info("Frontier listening on {}", server.getPort());

            return this;
        } catch (IOException ex) {
            shutdown();
            throw new UncheckedIOException(ex);
        }
    }

    public void shutdown() {
        healthCheckerExecutorService.shutdownNow();
        try {
            healthCheckerExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for health checker shutdown");
        }
        health.enterTerminalState();

        long startTime = System.currentTimeMillis();
        server.shutdown();
        frontierService.shutdown();
        try {
            server.awaitTermination(shutdownTimeoutMillis - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for server shutdown");
        }
        try {
            boolean gracefulStop = frontierService.awaitTermination(shutdownTimeoutMillis - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
            if (gracefulStop) {
                System.err.println("*** server shut down gracefully");
            } else {
                System.err.println("*** server shutdown timed out");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() {
        if (server != null) {
            try {
                frontierService.awaitTermination();
            } catch (InterruptedException ex) {
                shutdown();
                throw new RuntimeException(ex);
            }
        }
    }

    class HealthChecker implements Runnable {
        private final Frontier frontier;
        private final HealthStatusManager health;

        public HealthChecker(Frontier frontier, HealthStatusManager health) {
            this.frontier = frontier;
            this.health = health;
        }

        @Override
        public void run() {
            health.setStatus("", frontier.checkHealth());
        }
    }
}
