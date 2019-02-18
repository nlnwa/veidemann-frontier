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

package no.nb.nna.veidemann.frontier;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc.FrontierStub;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.harvester.browsercontroller.RenderResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HarvesterMock {
    private static final int NUM_HARVESTERS = 100;
    private static final Logger LOG = LoggerFactory.getLogger(HarvesterMock.class);
    private final static PageHarvest NEW_PAGE_REQUEST = PageHarvest.newBuilder().setRequestNextPage(true).build();
    private final FrontierGrpc.FrontierStub frontierAsyncStub;
    private final ExecutorService exe;

    public HarvesterMock(FrontierStub frontierAsyncStub) {
        this.frontierAsyncStub = frontierAsyncStub;
        exe = Executors.newFixedThreadPool(NUM_HARVESTERS);
    }

    public void start() {
        for (int i = 0; i < NUM_HARVESTERS; i++) {
            exe.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Harvester h = new Harvester();
                    while (true) {
                        h.harvest();
                    }
                }
            });
        }
    }

    private class Harvester {
        public void harvest() throws InterruptedException {
            CountDownLatch lock = new CountDownLatch(1);

            ResponseObserver responseObserver = new ResponseObserver(lock);

            StreamObserver<PageHarvest> requestObserver = frontierAsyncStub
                    .getNextPage(responseObserver);
            responseObserver.setRequestObserver(requestObserver);

            try {
                requestObserver.onNext(NEW_PAGE_REQUEST);
            } catch (RuntimeException e) {
                // Cancel RPC
                requestObserver.onError(e);
            }
            lock.await();
        }

        private class ResponseObserver implements StreamObserver<PageHarvestSpec> {
            final CountDownLatch lock;
            StreamObserver<PageHarvest> requestObserver;

            public ResponseObserver(CountDownLatch lock) {
                this.lock = lock;
            }

            public void setRequestObserver(StreamObserver<PageHarvest> requestObserver) {
                this.requestObserver = requestObserver;
            }

            @Override
            public void onNext(PageHarvestSpec pageHarvestSpec) {
                QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//            System.out.println("Harvest: " + fetchUri.getUri());

                List<QueuedUri> outlinks = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    QueuedUri.Builder qUri = QueuedUri.newBuilder()
                            .setUri(fetchUri.getUri() + "/p" + i)
                            .setDiscoveryPath(fetchUri.getDiscoveryPath() + "L")
                            .setJobExecutionId(fetchUri.getJobExecutionId())
                            .setExecutionId(fetchUri.getExecutionId())
                            .setCrawlHostGroupId(fetchUri.getCrawlHostGroupId())
                            .setIp(fetchUri.getIp());
                    outlinks.add(qUri.build());
                }
                try {
                    RenderResult result = new RenderResult()
                            .withBytesDownloaded(10l)
                            .withPageFetchTimeMs(10l)
                            .withUriCount(4)
                            .withOutlinks(outlinks);

                    PageHarvest.Builder reply = PageHarvest.newBuilder();

                    if (result.hasError()) {
                        reply.setError(result.getError());
                        requestObserver.onNext(reply.build());
                    } else {
                        reply.getMetricsBuilder()
                                .setBytesDownloaded(result.getBytesDownloaded())
                                .setUriCount(result.getUriCount());
                        requestObserver.onNext(reply.build());

                        result.getOutlinks().forEach(ol -> {
                            requestObserver.onNext(PageHarvest.newBuilder().setOutlink(ol).build());
                        });
                    }

                    requestObserver.onCompleted();
                } catch (Exception t) {
                    PageHarvest.Builder reply = PageHarvest.newBuilder();
                    reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
                    requestObserver.onNext(reply.build());
                    requestObserver.onCompleted();
                } finally {
                    lock.countDown();
                }
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                if (status.getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                    LOG.warn("Deadline expired while talking to the frontier", status);
                } else {
                    LOG.warn("Get next page failed: {}", status);
                }
            }

            @Override
            public void onCompleted() {
            }
        }
    }
}
