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

package no.nb.nna.veidemann.frontier.api;


import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FrontierClientMock implements AutoCloseable {
    final static int SUCCESS = 0;
    final static int ERROR = 1;
    final static int EXCEPTION = 2;
    final static int SUCCESS_WITH_OUTLINKS = 3;

    private final ManagedChannel channel;

    private final FrontierGrpc.FrontierStub asyncStub;
    private final FrontierGrpc.FrontierBlockingStub blockingStub;

    /**
     * Construct client for accessing Frontier using the existing channel.
     */
    public FrontierClientMock(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        asyncStub = FrontierGrpc.newStub(channel).withWaitForReady();
        blockingStub = FrontierGrpc.newBlockingStub(channel).withWaitForReady();
    }

    public PageHarvestSpec requestNext() {
        return blockingStub.getNextPage(Empty.getDefaultInstance());
    }

    public void pageCompleted(PageHarvestSpec pageHarvestSpec, int responseType) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<Empty> responseObserver = new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty value) {
            }

            @Override
            public void onError(Throwable t) {
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        };

        StreamObserver<PageHarvest> requestObserver = asyncStub.pageCompleted(responseObserver);

        QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();
        try {
            PageHarvest.Builder reply = PageHarvest.newBuilder().setSessionToken(pageHarvestSpec.getSessionToken());

            switch (responseType) {
                case ERROR:
                    reply.setError(Error.newBuilder().setCode(1).setMsg("Error").build());
                    requestObserver.onNext(reply.build());
                    break;
                case SUCCESS:
                    reply.getMetricsBuilder();
                    requestObserver.onNext(reply.build());
                    break;
                case SUCCESS_WITH_OUTLINKS:
                    reply.getMetricsBuilder();
                    requestObserver.onNext(reply.build());
                    requestObserver.onNext(PageHarvest.newBuilder().setOutlink(QueuedUri.newBuilder().setUri("http://example.com")).build());
                    break;
                case EXCEPTION:
                    throw new RuntimeException("Simulated render exception");
            }

            requestObserver.onCompleted();
        } catch (Exception t) {
            PageHarvest.Builder reply = PageHarvest.newBuilder();
            reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
            requestObserver.onNext(reply.build());
            requestObserver.onCompleted();
        }
        requestObserver.onCompleted();
        finishLatch.await(1, TimeUnit.MINUTES);
    }

    @Override
    public void close() {
        try {
            boolean isTerminated = channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            if (!isTerminated) {
                System.out.println("Harvester client has open connections after close");
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
