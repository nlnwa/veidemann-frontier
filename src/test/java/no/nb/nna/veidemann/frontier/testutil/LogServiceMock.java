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
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.log.v1.CrawlLog;
import no.nb.nna.veidemann.api.log.v1.LogGrpc;
import no.nb.nna.veidemann.api.log.v1.WriteCrawlLogRequest;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LogServiceMock implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogServiceMock.class);

    public final List<CrawlLog> crawlLogs = Collections.synchronizedList(new ArrayList<>());

    final Server server;

    public LogServiceMock(int port) {
        server = ServerBuilder.forPort(port)
                .addService(new LogService()).build();
    }

    public LogServiceMock start() throws IOException {
        server.start();
        return this;
    }

    @Override
    public void close() throws Exception {
        try {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            server.shutdownNow();
        }
    }

    public class LogService extends LogGrpc.LogImplBase {
        @Override
        public StreamObserver<WriteCrawlLogRequest> writeCrawlLog(StreamObserver<Empty> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(WriteCrawlLogRequest writeCrawlLogRequest) {
                    crawlLogs.add(writeCrawlLogRequest.getCrawlLog());
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error("Error writing crawl logs", throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
       }
    }
}
