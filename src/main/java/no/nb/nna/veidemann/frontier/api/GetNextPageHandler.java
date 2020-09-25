package no.nb.nna.veidemann.frontier.api;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.CrawlableUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.api.Context.RequestContext;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.RESCHEDULE_DELAY;

public class GetNextPageHandler implements StreamObserver<PageHarvest> {
    private static final Logger LOG = LoggerFactory.getLogger(GetNextPageHandler.class);
    static ExecutorService threadPool = Executors.newFixedThreadPool(16);
    CrawlExecution exe;
    final RequestContext ctx;
    final ServerCallStreamObserver responseObserver;

    public GetNextPageHandler(RequestContext ctx) {
        this.responseObserver = ctx.getResponseObserver();
        this.ctx = ctx;
    }

    public void sendError() {
        try {
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
                LOG.trace("Got request for new URI");
                if (ctx.isCancelled()) {
                    responseObserver.onError(Status.UNAVAILABLE.asException());
                    ctx.setObserverCompleted();
                    return;
                }
                try {
                    threadPool.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            try {
                                PageHarvestSpec pageHarvestSpec = null;
                                while (pageHarvestSpec == null) {
                                    while (exe == null && !ctx.isCancelled()) {
                                        CrawlableUri cUri = ctx.getCrawlQueueManager().getNextToFetch(ctx);
                                        if (ctx.isCancelled()) {
                                            LOG.debug("Context cancelled");
                                            if (cUri != null) {
                                                ctx.getCrawlQueueManager().releaseCrawlHostGroup(cUri.getCrawlHostGroup(), RESCHEDULE_DELAY);
                                            }
                                            sendError();
                                            return null;
                                        }
                                        exe = ctx.getCrawlQueueManager().createCrawlExecution(ctx, cUri);
                                    }
                                    if (exe == null) {
                                        sendError();
                                        return null;
                                    }

                                    LOG.trace("Found candidate URI {}", exe.getUri());
                                    pageHarvestSpec = exe.preFetch();
                                    if (pageHarvestSpec == null) {
                                        LOG.trace("Prefetch denied fetch of {}", exe.getUri());
                                        exe.postFetchFinally();
                                        exe = null;
                                    }
                                }

                                ctx.startPageFetch();
                                try {
                                    responseObserver.onNext(pageHarvestSpec);
                                } catch (StatusRuntimeException e) {
                                    if (e.getStatus().getCode() == Code.CANCELLED) {
                                        try {
                                            exe.postFetchFailure(ExtraStatusCodes.CANCELED_BY_BROWSER.toFetchError("Browser controller canceled request"));
                                        } catch (DbException e2) {
                                            LOG.error("Could not handle failure", e2);
                                        }
                                        exe.postFetchFinally();
                                        sendError();
                                        return null;
                                    } else {
                                        throw e;
                                    }
                                }
                            } catch (Exception e) {
                                LOG.error("Error preparing new fetch: {}", e.toString(), e);
                                exe.postFetchFinally();
                                sendError();
                            }
                            return null;
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                    sendError();
                }
                break;
            case METRICS:
                try {
                    exe.postFetchSuccess(value.getMetrics());
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchSuccess: {}", e.toString(), e);
                }
                break;
            case OUTLINK:
                try {
                    exe.queueOutlink(value.getOutlink());
                } catch (Exception e) {
                    LOG.warn("Could not queue outlink '{}'", value.getOutlink().getUri(), e);
                }
                break;
            case ERROR:
                try {
                    exe.postFetchFailure(value.getError());
                } catch (Exception e) {
                    LOG.warn("Failed to execute postFetchFailure: {}", e.toString(), e);
                }
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            try {
                if (exe != null) {
                    if (t instanceof StatusRuntimeException && ((StatusRuntimeException) t).getStatus().getCode() == Code.CANCELLED) {
                        exe.postFetchFailure(ExtraStatusCodes.CANCELED_BY_BROWSER.toFetchError("Browser controller canceled request"));
                    } else {
                        exe.postFetchFailure(t);
                    }
                } else {
                    LOG.debug("Error before any action {}", t.getMessage());
                }
            } catch (DbException e) {
                LOG.error("Could not handle failure", e);
            }
            try {
                if (exe != null) {
                    exe.postFetchFinally();
                    exe = null;
                }
            } catch (Exception e) {
                LOG.error("Failed to execute postFetchFinally after error: {}", e.toString(), e);
            }
        } finally {
            ctx.setObserverCompleted();
        }
    }

    @Override
    public void onCompleted() {
        try {
            try {
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Failed to execute onCompleted: {}", e.toString(), e);
            }
            try {
                exe.postFetchFinally();
                LOG.trace("Done with uri {}", exe.getUri().getUri());
                exe = null;
            } catch (Exception e) {
                LOG.error("Failed to execute postFetchFinally: {}", e.toString(), e);
            }
        } finally {
            ctx.setObserverCompleted();
        }
    }
}
