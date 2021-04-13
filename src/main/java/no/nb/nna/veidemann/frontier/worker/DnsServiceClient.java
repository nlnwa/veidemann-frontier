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

package no.nb.nna.veidemann.frontier.worker;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.dnsresolver.v1.DnsResolverGrpc;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveReply;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveRequest;
import no.nb.nna.veidemann.commons.client.GrpcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DnsServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DnsServiceClient.class);

    private final ManagedChannel channel;
    private final DnsResolverGrpc.DnsResolverFutureStub futureStub;
    private final ExecutorService executor;

    public DnsServiceClient(final String host, final int port, ExecutorService executor) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(), executor);
        LOG.info("DNS service client pointing to " + host + ":" + port);
    }

    public DnsServiceClient(ManagedChannelBuilder<?> channelBuilder, ExecutorService executor) {
        LOG.debug("Setting up DNS service client");
//        TODO: Add tracing
//        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
//        channel = channelBuilder.intercept(tracingInterceptor).build();
        channel = channelBuilder.build();
        futureStub = DnsResolverGrpc.newFutureStub(channel);
        this.executor = executor;
    }

    public ListenableFuture<InetSocketAddress> resolve(String host, int port, String executionId, ConfigRef collectionRef) {
        // Ensure host is never null
        String hostName = host == null ? "" : host;
        Objects.requireNonNull(collectionRef, "CollectionRef cannot be null");
        ResolveRequest request = ResolveRequest.newBuilder()
                .setHost(hostName)
                .setPort(port)
                .setExecutionId(executionId)
                .setCollectionRef(collectionRef)
                .build();

        ListenableFuture<ResolveReply> reply = GrpcUtil.forkedCall(() -> futureStub.resolve(request));

        reply = Futures.catchingAsync(reply, Exception.class, e -> {

            if (e instanceof StatusRuntimeException) {
                StatusRuntimeException ex = (StatusRuntimeException) e;
                if (ex.getStatus().getCode() == Status.UNAVAILABLE.getCode()) {
                    LOG.error("RPC failed: " + ex.getStatus(), ex);
                } else {
                    LOG.debug("RPC failed: " + ex.getStatus());
                }
                UnknownHostException err = new UnknownHostException(hostName);
                err.initCause(ex);
                throw err;
            } else {
                throw e;
            }
        }, MoreExecutors.directExecutor());

        ListenableFuture<InetSocketAddress> address = Futures.transformAsync(reply,
                r -> Futures.immediateFuture(
                        new InetSocketAddress(InetAddress.getByAddress(r.getHost(), r.getRawIp().toByteArray()), r.getPort())
                ), executor);

        return address;
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
