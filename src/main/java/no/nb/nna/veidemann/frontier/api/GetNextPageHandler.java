package no.nb.nna.veidemann.frontier.api;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.frontier.api.Context.RequestContext;
import no.nb.nna.veidemann.frontier.worker.IllegalSessionException;
import no.nb.nna.veidemann.frontier.worker.PostFetchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNextPageHandler implements StreamObserver<PageHarvest> {
    private static final Logger LOG = LoggerFactory.getLogger(GetNextPageHandler.class);
    final RequestContext ctx;
    final ServerCallStreamObserver responseObserver;
    private String sessionToken;
    private PostFetchHandler postFetchHandler;
    private Span span;

    public GetNextPageHandler(RequestContext ctx) {
        this.responseObserver = ctx.getResponseObserver();
        this.ctx = ctx;
        span = ctx.getFrontier().getTracer().scopeManager().activeSpan();
        if (span == null) {
            span = NoopSpan.INSTANCE;
        }
    }

    public void sendError() {
        try {
            sessionToken = null;
            responseObserver.onError(Status.ABORTED.asException());
        } catch (Exception e) {
            // OK if this fails
        }
        ctx.setObserverCompleted();
    }

    @Override
    public void onNext(PageHarvest value) {
        switch (value.getMsgCase()) {
            case REQUESTNEXTPAGE:
                span = ctx.getFrontier().getTracer().buildSpan("requestNextPage")
                        .withTag(Tags.COMPONENT, "Frontier")
                        .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
                        .start();

                try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
                    LOG.trace("Got request for new URI");
                    if (ctx.isCancelled()) {
                        responseObserver.onError(Status.UNAVAILABLE.asException());
                        ctx.setObserverCompleted();
                        return;
                    }
                    try {
                        PageHarvestSpec pageHarvestSpec = null;
                        while (pageHarvestSpec == null && !ctx.isCancelled()) {
                            pageHarvestSpec = ctx.getCrawlQueueManager().getNextToFetch(ctx);
                            if (pageHarvestSpec == null || ctx.isCancelled()) {
                                LOG.trace("Context cancelled");
                                sendError();
                                return;
                            }
                        }
                        sessionToken = pageHarvestSpec.getSessionToken();
                        span.setTag("uri", pageHarvestSpec.getQueuedUri().getUri());
                        span.setTag("sessionToken", sessionToken);
                        ctx.startPageFetch();
                        try {
                            responseObserver.onNext(pageHarvestSpec);
                        } catch (StatusRuntimeException e) {
                            if (e.getStatus().getCode() == Code.CANCELLED) {
                                sendError();
                                return;
                            } else {
                                throw e;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error preparing new fetch: {}", e.toString(), e);
                        sendError();
                    }
                    break;
                } finally {
                    span.finish();
                }
            case METRICS:
                try {
                    if (postFetchHandler == null) {
                        postFetchHandler = new PostFetchHandler(sessionToken, ctx.getFrontier());
                    }
                    ctx.setFetchCompleted();
                    postFetchHandler.postFetchSuccess(value.getMetrics());
                } catch (IllegalSessionException e) {
                    sendError();
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchSuccess: {}", e.toString(), e);
                }
                break;
            case OUTLINK:
                try {
                    if (postFetchHandler == null) {
                        postFetchHandler = new PostFetchHandler(sessionToken, ctx.getFrontier());
                    }
                    ctx.setFetchCompleted();
                    postFetchHandler.queueOutlink(value.getOutlink());
                } catch (IllegalSessionException e) {
                    sendError();
                } catch (Exception e) {
                    LOG.warn("Could not queue outlink '{}'", value.getOutlink().getUri(), e);
                }
                break;
            case ERROR:
                try {
                    if (postFetchHandler == null) {
                        postFetchHandler = new PostFetchHandler(sessionToken, ctx.getFrontier());
                    }
                    ctx.setFetchCompleted();
                    postFetchHandler.postFetchFailure(value.getError());
                } catch (IllegalSessionException e) {
                    sendError();
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchFailure: {}", e.toString(), e);
                }
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        if (sessionToken == null || (t instanceof StatusRuntimeException && ((StatusRuntimeException) t).getStatus().getCode() == Code.CANCELLED)) {
            ctx.setObserverCompleted();
            return;
        }

        LOG.warn("gRPC Error from harvester (Session token: {})", sessionToken, t);
        try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
            try {
                if (postFetchHandler == null) {
                    postFetchHandler = new PostFetchHandler(sessionToken, ctx.getFrontier());
                }
                ctx.setFetchCompleted();
                Error error = ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError("Browser controller failed: " + t.toString());
                postFetchHandler.postFetchFailure(error);
                postFetchHandler.postFetchFinally();
            } catch (Exception e) {
                LOG.error("Failed to execute postFetchFinally after error: {}", e.toString(), e);
            }
        } finally {
            ctx.setObserverCompleted();
        }
    }

    @Override
    public void onCompleted() {
        Span span = ctx.getFrontier().getTracer().buildSpan("completeFetch")
                .withTag(Tags.COMPONENT, "Frontier")
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
                .start();

        try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
            try {
                if (postFetchHandler == null) {
                    postFetchHandler = new PostFetchHandler(sessionToken, ctx.getFrontier());
                }
                ctx.setFetchCompleted();
                postFetchHandler.postFetchFinally();
                LOG.trace("Done with uri {}", postFetchHandler.getUri().getUri());
                postFetchHandler = null;
            } catch (IllegalSessionException e) {
                sendError();
                return;
            } catch (Exception e) {
                LOG.error("Failed to execute postFetchFinally: {}", e.toString(), e);
            }
            try {
                if (!responseObserver.isCancelled()) {
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                LOG.error("Failed to execute onCompleted: {}", e.toString(), e);
            }
        } finally {
            ctx.setObserverCompleted();
            span.finish();
        }
    }
}
