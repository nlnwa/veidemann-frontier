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

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.frontier.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HarvesterMock implements AutoCloseable {
    private static final int NUM_HARVESTERS = 400;
    private static final Logger LOG = LoggerFactory.getLogger(HarvesterMock.class);

    public RequestLog requestLog = new RequestLog();
    private final RequestMatcher exceptionForUrl = new RequestMatcher(requestLog);
    private final RequestMatcher fetchErrorForUrl = new RequestMatcher(requestLog);
    private final RequestMatcher longFetchTimeForUrl = new RequestMatcher(requestLog);
    int linksPerLevel = 0;
    int outOfScopeLinksPerLevel = 0;
    long pageFetchTimeMs = 10;
    long longPageFetchTimeMs = 5000;

    private final ManagedChannel frontierChannel;
    private final FrontierGrpc.FrontierStub frontierAsyncStub;
    private final FrontierGrpc.FrontierBlockingStub frontierBlockingStub;
    private ExecutorService nextPageThreads;
    private ExecutorService pageCompletedThreads;
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);

    private final DelayQueue<Harvester> idleQueue = new DelayQueue<>();
    private final DelayQueue<Harvester> fetchQueue = new DelayQueue<>();
    private final Speed speed = new Speed();

    public HarvesterMock(Settings settings) {
        frontierChannel = ManagedChannelBuilder.forAddress("localhost", settings.getApiPort()).usePlaintext().build();
        frontierAsyncStub = FrontierGrpc.newStub(frontierChannel).withWaitForReady();
        frontierBlockingStub = FrontierGrpc.newBlockingStub(frontierChannel).withWaitForReady();
    }

    public HarvesterMock start() {
        nextPageThreads = Executors.newFixedThreadPool(8);
        pageCompletedThreads = Executors.newFixedThreadPool(16);
        for (int i = 0; i < NUM_HARVESTERS; i++) {
            idleQueue.add(new Harvester());
        }
        nextPageThreads.submit((Callable<Void>) () -> {
            while (shouldRun.get()) {
                try {
                    nextPageThreads.submit((Runnable) idleQueue.take());
                } catch (InterruptedException e) {
                }
            }
            return null;
        });
        pageCompletedThreads.submit((Callable<Void>) () -> {
            while (shouldRun.get()) {
                try {
                    pageCompletedThreads.submit((Runnable) fetchQueue.take());
                } catch (InterruptedException e) {
                }
            }
            return null;
        });

        return this;
    }

    public void close() throws InterruptedException {
        shouldRun.set(false);
        frontierChannel.shutdownNow().awaitTermination(15, TimeUnit.SECONDS);
        nextPageThreads.shutdownNow();
        nextPageThreads.awaitTermination(15, TimeUnit.SECONDS);
        pageCompletedThreads.shutdownNow();
        pageCompletedThreads.awaitTermination(15, TimeUnit.SECONDS);
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

    private class Harvester implements Delayed, Runnable {
        long delay;
        long startTime;
        PageHarvestSpec pageHarvestSpec;

        void setDelay(long delayMs) {
            startTime = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(@NotNull Delayed o) {
            return (int) (startTime - ((Harvester) o).getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public void run() {
            if (pageHarvestSpec == null) {
                getNext();
            } else {
                pageCompleted();
            }
        }

        void getNext() {
            try {
                pageHarvestSpec = frontierBlockingStub.getNextPage(Empty.getDefaultInstance());

                // Add request to documentLog
                requestLog.addRequest(pageHarvestSpec.getQueuedUri().getUri());

                setDelay(pageFetchTimeMs);
                fetchQueue.add(this);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Code.NOT_FOUND || e.getStatus().getCode() == Code.CANCELLED) {
                    if (delay < 100) {
                        delay = 100;
                    } else {
                        delay = 2 * delay;
                    }
                    if (delay > 2000) {
                        delay = 2000;
                    }
                    setDelay(delay);
                    pageHarvestSpec = null;
                    idleQueue.add(this);
                } else if (e.getStatus().getCode() == Code.UNAVAILABLE) {
                    return;
                } else {
                    e.printStackTrace();
                    throw e;
                }
            }
        }

        void pageCompleted() {
            speed.inc();
            AtomicBoolean shouldRun = new AtomicBoolean(true);
            final CountDownLatch finishLatch = new CountDownLatch(1);
            StreamObserver<Empty> responseObserver = new StreamObserver<Empty>() {
                @Override
                public void onNext(Empty value) {
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("ON ERROR " + t.toString());
                    shouldRun.set(false);
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    finishLatch.countDown();
                }
            };

            StreamObserver<PageHarvest> requestObserver = frontierAsyncStub.pageCompleted(responseObserver);

            QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();

            try {
                PageHarvest.Builder reply = PageHarvest.newBuilder().setSessionToken(pageHarvestSpec.getSessionToken());

                // Simulate bug in harvester when crawling url
                if (exceptionForUrl.match(fetchUri.getUri())) {
                    throw new RuntimeException("Simulated bug in harvester");
                }

                // Simulate failed page fetch when crawling url
                if (fetchErrorForUrl.match(fetchUri.getUri())) {
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
                    reply.getMetricsBuilder()
                            .setBytesDownloaded(10l)
                            .setUriCount(4);
                    if (shouldRun.get()) {
                        requestObserver.onNext(reply.build());
                    }

                    outlinks.forEach(ol -> {
                        if (shouldRun.get()) {
                            requestObserver.onNext(PageHarvest.newBuilder().setOutlink(ol).build());
                        }
                    });

                } catch (Exception t) {
                    reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
                    requestObserver.onNext(reply.build());
                }

            } catch (Exception t) {
                t.printStackTrace();
                PageHarvest.Builder reply = PageHarvest.newBuilder();
                reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
                requestObserver.onNext(reply.build());
            }

            requestObserver.onCompleted();
            try {
                finishLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            pageHarvestSpec = null;
            delay = 0;
            setDelay(delay);
            idleQueue.add(this);
        }
    }

    private class Speed {
        long startTime = System.currentTimeMillis();
        long count;

        double pps() {
            long now = System.currentTimeMillis();
            double res = ((double) count / (double) (now - startTime)) * 1000d;
            startTime = System.currentTimeMillis();
            count = 0;
            return res;
        }

        void inc() {
            count++;
            if (System.currentTimeMillis() - startTime > 5000) {
                LOG.debug("Pages per second: {}", pps());
            }
        }
    }
}
