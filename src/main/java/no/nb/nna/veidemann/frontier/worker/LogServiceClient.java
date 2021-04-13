/*
 * Copyright 2021 National Library of Norway.
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
package no.nb.nna.veidemann.frontier.worker;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.log.v1.CrawlLog;
import no.nb.nna.veidemann.api.log.v1.LogGrpc;
import no.nb.nna.veidemann.api.log.v1.LogGrpc.LogStub;
import no.nb.nna.veidemann.api.log.v1.WriteCrawlLogRequest;
import no.nb.nna.veidemann.commons.client.GrpcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LogServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LogServiceClient.class);

    private final ManagedChannel channel;

    private final LogStub logStub;

    public LogServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("LogService client pointing to " + host + ":" + port);
    }

    public LogServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        logStub = LogGrpc.newStub(channel);
    }

    public void writeCrawlLog(CrawlLog crawlLog) {
        StreamObserver<WriteCrawlLogRequest> writeObserver = GrpcUtil.forkedCall(() -> logStub.writeCrawlLog(new StreamObserver<>() {
            @Override
            public void onNext(Empty empty) {
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Error writing crawlLog: {}", throwable.toString(), throwable);
            }

            @Override
            public void onCompleted() {
            }
        }));

        WriteCrawlLogRequest writeCrawlLogRequest = WriteCrawlLogRequest.newBuilder()
                .setCrawlLog(crawlLog)
                .build();
        writeObserver.onNext(writeCrawlLogRequest);
        writeObserver.onCompleted();
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            channel.shutdownNow();
        }
    }
}
