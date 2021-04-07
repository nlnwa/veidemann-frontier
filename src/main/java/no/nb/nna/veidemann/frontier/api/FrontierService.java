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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FrontierService extends FrontierGrpc.FrontierImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierService.class);

    final Context ctx;

    public FrontierService(Frontier frontier) {
        ctx = new Context(frontier);
    }

    public void shutdown() {
        ctx.getFrontier().close();
        ctx.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return ctx.awaitTermination(timeout, unit);
    }

    public void awaitTermination() throws InterruptedException {
        ctx.awaitTermination();
    }

    @Override
    public void crawlSeed(CrawlSeedRequest request, StreamObserver<CrawlExecutionId> responseObserver) {
//        TODO: Add tracing
//        try (ActiveSpan span = GlobalTracer.get()
//                .buildSpan("scheduleSeed")
//                .asChildOf(OpenTracingContextKey.activeSpan())
//                .withTag(Tags.COMPONENT.getKey(), "Frontier")
//                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
//                .withTag("uri", request.getSeed().getMeta().getName())
//                .startActive()) {
        try {
            Futures.addCallback(ctx.getFrontier().scheduleSeed(request),
                    new FutureCallback<CrawlExecutionStatus>() {
                        public void onSuccess(CrawlExecutionStatus reply) {
                            responseObserver.onNext(CrawlExecutionId.newBuilder().setId(reply.getId()).build());
                            responseObserver.onCompleted();
                        }

                        public void onFailure(Throwable t) {
                            LOG.error("Crawl seed error: " + t.getMessage(), t);
                            Status status = Status.UNKNOWN.withDescription(t.toString());
                            responseObserver.onError(status.asException());
                        }
                    }, no.nb.nna.veidemann.frontier.FrontierService.asyncFunctionsExecutor);
        } catch (Exception e) {
            LOG.error("Crawl seed error: " + e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public StreamObserver<PageHarvest> getNextPage(StreamObserver<PageHarvestSpec> responseObserver) {
        return new GetNextPageHandler(ctx.newRequestContext((ServerCallStreamObserver) responseObserver));
    }

    @Override
    public void busyCrawlHostGroupCount(Empty request, StreamObserver<CountResponse> responseObserver) {
        CountResponse response = CountResponse.newBuilder()
                .setCount(ctx.getCrawlQueueManager().busyCrawlHostGroupCount())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queueCountTotal(Empty request, StreamObserver<CountResponse> responseObserver) {
        CountResponse response = CountResponse.newBuilder()
                .setCount(ctx.getCrawlQueueManager().queueCountTotal())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queueCountForCrawlExecution(CrawlExecutionId request, StreamObserver<CountResponse> responseObserver) {
        CountResponse response = CountResponse.newBuilder()
                .setCount(ctx.getCrawlQueueManager().countByCrawlExecution(request.getId()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queueCountForCrawlHostGroup(CrawlHostGroup request, StreamObserver<CountResponse> responseObserver) {
        CountResponse response = CountResponse.newBuilder()
                .setCount(ctx.getCrawlQueueManager().countByCrawlHostGroup(request))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
