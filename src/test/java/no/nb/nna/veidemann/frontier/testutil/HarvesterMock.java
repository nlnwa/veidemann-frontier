/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.frontier.testutil;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.frontier.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HarvesterMock implements AutoCloseable {
    private static final int NUM_HARVESTERS = 200;
    private static final Logger LOG = LoggerFactory.getLogger(HarvesterMock.class);

    public RequestLog requestLog = new RequestLog();
    private final RequestMatcher exceptionForUrl = new RequestMatcher(requestLog);
    private final RequestMatcher fetchErrorForUrl = new RequestMatcher(requestLog);
    private final RequestMatcher longFetchTimeForUrl = new RequestMatcher(requestLog);
    int linksPerLevel = 0;
    int outOfScopeLinksPerLevel = 0;
    long pageFetchTimeMs = 10;
    long longPageFetchTimeMs = 5000;

    private final static PageHarvest NEW_PAGE_REQUEST = PageHarvest.newBuilder().setRequestNextPage(true).build();
    private final ManagedChannel frontierChannel;
    private final FrontierGrpc.FrontierStub frontierAsyncStub;
    private ExecutorService exe;
    private AtomicBoolean shouldRun = new AtomicBoolean(true);

    public HarvesterMock(Settings settings) {
        frontierChannel = ManagedChannelBuilder.forAddress("localhost", settings.getApiPort()).usePlaintext().build();
        frontierAsyncStub = FrontierGrpc.newStub(frontierChannel).withWaitForReady();
    }

    public HarvesterMock start() {
        exe = Executors.newFixedThreadPool(NUM_HARVESTERS);
        for (int i = 0; i < NUM_HARVESTERS; i++) {
            exe.submit((Callable<Void>) () -> {
                Harvester h = new Harvester();
                while (shouldRun.get()) {
                    h.harvest();
                    Thread.sleep(50);
                }
                return null;
            });
        }
        return this;
    }

    public void close() throws InterruptedException {
        shouldRun.set(false);
        frontierChannel.shutdownNow().awaitTermination(15, TimeUnit.SECONDS);
        exe.shutdownNow();
        exe.awaitTermination(15, TimeUnit.SECONDS);
    }

    public HarvesterMock withFetchTime(long millis) {
        this.pageFetchTimeMs = millis;
        return this;
    }

    public HarvesterMock withLongFetchTime(long millis) {
        this.longPageFetchTimeMs = millis;
        return this;
    }

    public HarvesterMock withExceptionForAllUrlRequests(String url) {
        this.exceptionForUrl.withMatchAllRequests(url);
        return this;
    }

    public HarvesterMock withExceptionForUrlRequests(String url, int from, int to) {
        this.exceptionForUrl.withMatchRequests(url, from, to);
        return this;
    }

    public HarvesterMock withFetchErrorForAllUrlRequests(String url) {
        this.fetchErrorForUrl.withMatchAllRequests(url);
        return this;
    }

    public HarvesterMock withFetchErrorForUrlRequests(String url, int from, int to) {
        this.fetchErrorForUrl.withMatchRequests(url, from, to);
        return this;
    }

    public HarvesterMock withLongFetchTimeForAllUrlRequests(String url) {
        this.longFetchTimeForUrl.withMatchAllRequests(url);
        return this;
    }

    public HarvesterMock withLongFetchTimeForUrlRequests(String url, int from, int to) {
        this.longFetchTimeForUrl.withMatchRequests(url, from, to);
        return this;
    }

    public HarvesterMock withLinksPerLevel(int linksPerLevel) {
        this.linksPerLevel = linksPerLevel;
        return this;
    }

    public HarvesterMock withOutOfScopeLinksPerLevel(int outOfScopeLinksPerLevel) {
        this.outOfScopeLinksPerLevel = outOfScopeLinksPerLevel;
        return this;
    }

    private class Harvester {
        ClientCallStreamObserver<PageHarvest> requestObserver;
        CountDownLatch lock;

        public void harvest() throws InterruptedException {
            lock = new CountDownLatch(1);

            ResponseObserver responseObserver = new ResponseObserver(lock);
            requestObserver = (ClientCallStreamObserver<PageHarvest>) frontierAsyncStub
                    .getNextPage(responseObserver);

            try {
                requestObserver.onNext(NEW_PAGE_REQUEST);
            } catch (RuntimeException e) {
                // Cancel RPC
                requestObserver.onError(e);
            }
            lock.await();
            requestObserver = null;
            lock = null;
        }

        public void close() {
            requestObserver.onCompleted();
        }

        private class ResponseObserver implements StreamObserver<PageHarvestSpec> {
            final CountDownLatch lock;

            public ResponseObserver(CountDownLatch lock) {
                this.lock = lock;
            }

            @Override
            public void onNext(PageHarvestSpec pageHarvestSpec) {
                QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();

                // Add request to documentLog
                requestLog.addRequest(fetchUri.getUri());

                // Simulate bug in harvester when crawling url
                if (exceptionForUrl.match(fetchUri.getUri())) {
                    throw new RuntimeException("Simulated bug in harvester");
                }

                // Simulate failed page fetch when crawling url
                if (fetchErrorForUrl.match(fetchUri.getUri())) {
                    PageHarvest.Builder reply = PageHarvest.newBuilder();
                    reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION
                            .toFetchError(new RuntimeException("Simulated fetch error").toString()));
                    requestObserver.onNext(reply.build());
                    requestObserver.onCompleted();
                    return;
                }

                try {
                    if (longFetchTimeForUrl.match(fetchUri.getUri())) {
                        Thread.sleep(longPageFetchTimeMs);
                    } else {
                        Thread.sleep(pageFetchTimeMs);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                List<QueuedUri> outlinks = new ArrayList<>();
                // In Scope links
                for (int i = 0; i < linksPerLevel; i++) {
                    QueuedUri.Builder qUri = QueuedUri.newBuilder()
                            .setUri(fetchUri.getUri() + "/p" + (i % 5))
                            .setDiscoveryPath(fetchUri.getDiscoveryPath() + "L")
                            .setJobExecutionId(fetchUri.getJobExecutionId())
                            .setExecutionId(fetchUri.getExecutionId())
                            .setCrawlHostGroupId(fetchUri.getCrawlHostGroupId())
                            .setIp(fetchUri.getIp());
                    outlinks.add(qUri.build());
                }
                // Out of Scope scope links
                for (int i = 0; i < outOfScopeLinksPerLevel; i++) {
                    QueuedUri.Builder qUri = QueuedUri.newBuilder()
                            .setUri("http://www.example" + i + ".com/foo")
                            .setDiscoveryPath(fetchUri.getDiscoveryPath() + "L")
                            .setJobExecutionId(fetchUri.getJobExecutionId())
                            .setExecutionId(fetchUri.getExecutionId())
                            .setCrawlHostGroupId(fetchUri.getCrawlHostGroupId())
                            .setIp(fetchUri.getIp());
                    outlinks.add(qUri.build());
                }

                try {
                    PageHarvest.Builder reply = PageHarvest.newBuilder();

                    reply.getMetricsBuilder()
                            .setBytesDownloaded(10l)
                            .setUriCount(4);
                    requestObserver.onNext(reply.build());

                    outlinks.forEach(ol -> {
                        requestObserver.onNext(PageHarvest.newBuilder().setOutlink(ol).build());
                    });

                    requestObserver.onCompleted();
                } catch (Exception t) {
                    PageHarvest.Builder reply = PageHarvest.newBuilder();
                    reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
                    requestObserver.onNext(reply.build());
                    requestObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {
                lock.countDown();
            }

            @Override
            public void onCompleted() {
                lock.countDown();
            }
        }
    }
}
