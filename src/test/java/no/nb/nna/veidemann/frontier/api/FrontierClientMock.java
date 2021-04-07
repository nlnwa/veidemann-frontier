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


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;

import java.util.concurrent.TimeUnit;

public class FrontierClientMock implements AutoCloseable {
    final static int SUCCESS = 0;
    final static int ERROR = 1;
    final static int EXCEPTION = 2;
    final static int SUCCESS_WITH_OUTLINKS = 3;

    private final static PageHarvest NEW_PAGE_REQUEST = PageHarvest.newBuilder().setRequestNextPage(true).build();

    private final ManagedChannel channel;

    private final FrontierGrpc.FrontierStub asyncStub;

    /**
     * Construct client for accessing Frontier using the existing channel.
     */
    public FrontierClientMock(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        asyncStub = FrontierGrpc.newStub(channel).withWaitForReady();
    }

    public void requestNext(int responseType) {
        ResponseObserver responseObserver = new ResponseObserver(responseType);

        StreamObserver<PageHarvest> requestObserver = asyncStub.getNextPage(responseObserver);
        responseObserver.setRequestObserver(requestObserver);

        try {
            requestObserver.onNext(NEW_PAGE_REQUEST);
        } catch (RuntimeException e) {
            // Cancel RPC
            e.printStackTrace();
            requestObserver.onError(e);
        }
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

    private class ResponseObserver implements StreamObserver<PageHarvestSpec> {
        StreamObserver<PageHarvest> requestObserver;
        int respType;

        public ResponseObserver(int respType) {
            this.respType = respType;
        }

        public void setRequestObserver(StreamObserver<PageHarvest> requestObserver) {
            this.requestObserver = requestObserver;
        }

        @Override
        public void onNext(PageHarvestSpec pageHarvestSpec) {
            QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();
            try {
                PageHarvest.Builder reply = PageHarvest.newBuilder();

                switch (respType) {
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
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
        }

        @Override
        public void onCompleted() {
        }
    }
}
