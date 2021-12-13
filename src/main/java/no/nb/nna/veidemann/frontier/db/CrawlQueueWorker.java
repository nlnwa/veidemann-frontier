package no.nb.nna.veidemann.frontier.db;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.frontier.db.script.RedisJob.JedisContext;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.PostFetchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_TIMEOUT_KEY;

public class CrawlQueueWorker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlQueueWorker.class);

    private final Frontier frontier;
    private final JedisPool jedisPool;
    private final ScheduledExecutorService executor;

    Runnable fetchTimeoutWorker = new Runnable() {
        @Override
        public void run() {
            Error err = ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError("Timeout waiting for Harvester");

            try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
                for (String chgId = ctx.getJedis().lpop(CHG_TIMEOUT_KEY); chgId != null; chgId = ctx.getJedis().lpop(CHG_TIMEOUT_KEY)) {
                    try {
                        CrawlHostGroup chg = frontier.getCrawlQueueManager().getCrawlHostGroup(chgId);
                        if (chg.getCurrentUriId().isEmpty()) {
                            frontier.getCrawlQueueManager().releaseCrawlHostGroup(ctx, chgId, chg.getSessionToken(), 0, true);
                            continue;
                        }
                        PostFetchHandler postFetchHandler = new PostFetchHandler(chg, frontier, false);
                        postFetchHandler.postFetchFailure(err);
                        postFetchHandler.postFetchFinally(true);
                    } catch (Exception e) {
                        LOG.warn("Error while getting chg {}", chgId, e);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    };

    Runnable checkPaused = new Runnable() {
        @Override
        public void run() {
            try {
                frontier.getCrawlQueueManager().pause(DbService.getInstance().getExecutionsAdapter().getDesiredPausedState());
            } catch (DbException e) {
                LOG.warn("Could not read pause state", e);
            } catch (Exception e) {
                e.printStackTrace();
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    };

    public CrawlQueueWorker(Frontier frontier, JedisPool jedisPool) {
        this.frontier = frontier;
        this.jedisPool = jedisPool;
        executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("CrawlQueueWorker-%d").build());

        executor.scheduleWithFixedDelay(fetchTimeoutWorker, 1200, 500, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(checkPaused, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        LOG.debug("Closing CrawlQueueWorker");
        executor.shutdown();
        executor.awaitTermination(15, TimeUnit.SECONDS);
        LOG.debug("CrawlQueueWorker closed");
    }
}
