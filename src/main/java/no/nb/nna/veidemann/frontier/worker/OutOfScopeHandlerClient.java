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

package no.nb.nna.veidemann.frontier.worker;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.api.ooshandler.v1.OosHandlerGrpc;
import no.nb.nna.veidemann.api.ooshandler.v1.SubmitUriRequest;
import no.nb.nna.veidemann.commons.client.GrpcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class OutOfScopeHandlerClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OutOfScopeHandlerClient.class);

    private final ManagedChannel channel;

    private final OosHandlerGrpc.OosHandlerFutureStub futureStub;

    public OutOfScopeHandlerClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("Out of Scope Handler client pointing to " + host + ":" + port);
    }

    public OutOfScopeHandlerClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up Out of Scope Handler client");
        TracingClientInterceptor tracingInterceptor = TracingClientInterceptor.newBuilder().withTracer(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        futureStub = OosHandlerGrpc.newFutureStub(channel);
    }

    public void submitUri(QueuedUri uri) {
        try {
            SubmitUriRequest request = SubmitUriRequest.newBuilder()
                    .setUri(uri)
                    .build();

            GrpcUtil.forkedCall(() -> futureStub.submitUri(request));
        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode() == Code.UNAVAILABLE) {
                LOG.debug("RPC failed: " + ex.getStatus(), ex);
            } else {
                LOG.error("RPC failed: " + ex.getStatus(), ex);
            }
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

}
