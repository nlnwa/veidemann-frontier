package no.nb.nna.veidemann.frontier.api;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNextPageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GetNextPageHandler.class);
    private static final long TIMEOUT = 1000 * 30;

    public static void onNext(Context ctx, final StreamObserver<PageHarvestSpec> responseObserver) {
        long start = System.currentTimeMillis();

        Span span = ctx.getFrontier().getTracer().buildSpan("requestNextPage")
                .withTag(Tags.COMPONENT, "Frontier")
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
                .start();

        try (Scope scope = ctx.getFrontier().getTracer().scopeManager().activate(span)) {
            LOG.trace("Got request for new URI");
            try {
                PageHarvestSpec pageHarvestSpec = ctx.getFrontier().getCrawlQueueManager().getNextToFetch();
                if (pageHarvestSpec == null) {
                    responseObserver.onError(Status.NOT_FOUND.asException());
                    LOG.trace("Get next page request not found");
                    return;
                }

                String sessionToken = pageHarvestSpec.getSessionToken();
                span.setTag("uri", pageHarvestSpec.getQueuedUri().getUri());
                span.setTag("sessionToken", sessionToken);

                try {
                    responseObserver.onNext(pageHarvestSpec);
                    responseObserver.onCompleted();
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Code.CANCELLED) {
                        responseObserver.onError(Status.ABORTED.asException());
                        return;
                    } else {
                        throw e;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error preparing new fetch: {}", e.toString(), e);
                responseObserver.onError(Status.ABORTED.asException());
            }
        } finally {
            span.finish();
        }
    }

    public static boolean isTimeout(long startTime) {
        return System.currentTimeMillis() - startTime > TIMEOUT;
    }
}
