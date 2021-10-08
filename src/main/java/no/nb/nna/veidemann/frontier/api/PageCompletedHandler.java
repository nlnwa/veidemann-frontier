package no.nb.nna.veidemann.frontier.api;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.frontier.worker.IllegalSessionException;
import no.nb.nna.veidemann.frontier.worker.PostFetchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageCompletedHandler implements StreamObserver<PageHarvest> {
    private static final Logger LOG = LoggerFactory.getLogger(PageCompletedHandler.class);
    final Context ctx;
    final StreamObserver<Empty> responseObserver;
    private PostFetchHandler postFetchHandler;
    private Span span;

    public PageCompletedHandler(Context ctx, StreamObserver<Empty> responseObserver) {
        this.responseObserver = responseObserver;
        this.ctx = ctx;
        span = ctx.getFrontier().getTracer().scopeManager().activeSpan();
        if (span == null) {
            span = NoopSpan.INSTANCE;
        }
        ctx.startPageComplete();
    }

    public void sendError() {
        try {
            postFetchHandler = null;
            responseObserver.onError(Status.ABORTED.asException());
        } catch (Exception e) {
            // OK if this fails
        }
    }

    @Override
    public void onNext(PageHarvest value) {
        if (postFetchHandler == null) {
            try {
                postFetchHandler = new PostFetchHandler(value.getSessionToken(), ctx.getFrontier());
            } catch (IllegalSessionException e) {
                postFetchHandler = null;
                sendError();
                return;
            } catch (Exception e) {
                LOG.warn("Failed to load postFetchHandler: {}", e.toString(), e);
                postFetchHandler = null;
                sendError();
                return;
            }
        }
        switch (value.getMsgCase()) {
            case METRICS:
                try {
                    postFetchHandler.postFetchSuccess(value.getMetrics());
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchSuccess: {}", e.toString(), e);
                    sendError();
                }
                break;
            case OUTLINK:
                try {
                    postFetchHandler.queueOutlink(value.getOutlink());
                } catch (Exception e) {
                    LOG.warn("Could not queue outlink '{}'", value.getOutlink().getUri(), e);
                    sendError();
                }
                break;
            case ERROR:
                try {
                    postFetchHandler.postFetchFailure(value.getError());
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchFailure: {}", e.toString(), e);
                    sendError();
                }
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        if (postFetchHandler == null || (t instanceof StatusRuntimeException && ((StatusRuntimeException) t).getStatus().getCode() == Code.CANCELLED)) {
            ctx.setObserverCompleted();
            postFetchHandler = null;
            return;
        }

        LOG.warn("gRPC Error from harvester", t);
        try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
            try {
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
        if (postFetchHandler == null) {
            return;
        }

        Span span = ctx.getFrontier().getTracer().buildSpan("completeFetch")
                .withTag(Tags.COMPONENT, "Frontier")
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
                .start();

        try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
            try {
                postFetchHandler.postFetchFinally();
                LOG.trace("Done with uri {}", postFetchHandler.getUri().getUri());
                postFetchHandler = null;
            } catch (Exception e) {
                LOG.error("Failed to execute postFetchFinally: {}", e.toString(), e);
                sendError();
                return;
            }
            try {
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Failed to execute onCompleted: {}", e.toString(), e);
            }
        } finally {
            ctx.setObserverCompleted();
            span.finish();
        }
    }
}
