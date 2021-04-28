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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedReply;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedRequest;
import no.nb.nna.veidemann.api.robotsevaluator.v1.RobotsEvaluatorGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RobotsEvaluatorMock implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RobotsEvaluatorMock.class);

    private final List<String> exceptionForUrl = new ArrayList<>();
    private final List<String> fetchDenialForUrl = new ArrayList<>();

    final Server server;

    public RobotsEvaluatorMock(int port) {
        server = ServerBuilder.forPort(port).addService(new RobotsService()).build();
    }

    public RobotsEvaluatorMock start() throws IOException {
        server.start();
        return this;
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    public RobotsEvaluatorMock withExceptionForUrl(String url) {
        this.exceptionForUrl.add(url);
        return this;
    }

    public RobotsEvaluatorMock withFetchDenialForUrl(String url) {
        this.fetchDenialForUrl.add(url);
        return this;
    }

    public class RobotsService extends RobotsEvaluatorGrpc.RobotsEvaluatorImplBase {
        @Override
        public void isAllowed(IsAllowedRequest request, StreamObserver<IsAllowedReply> responseObserver) {
            // Simulate bug in RobotsEvaluator when when checking uri
            if (exceptionForUrl.contains(request.getUri())) {
                LOG.info("RobotsEvaluator failed for {}", request.getUri());
                throw new RuntimeException("Simulated bug in RobotsEvaluator");
            }

            // Simulate denial for url
            if (fetchDenialForUrl.contains(request.getUri())) {
                LOG.info("Harvest of {} denied by RobotsEvaluator", request.getUri());
                responseObserver.onNext(IsAllowedReply.newBuilder().setIsAllowed(false).build());
            } else {
                responseObserver.onNext(IsAllowedReply.newBuilder().setIsAllowed(true).build());
            }
            responseObserver.onCompleted();
        }
    }
}
