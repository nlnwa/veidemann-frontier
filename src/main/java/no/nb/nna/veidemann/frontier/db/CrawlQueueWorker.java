package no.nb.nna.veidemann.frontier.db;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.frontier.db.script.ChgBusyTimeoutScript;
import no.nb.nna.veidemann.frontier.db.script.ChgDelayedQueueScript;
import no.nb.nna.veidemann.frontier.db.script.RedisJob.JedisContext;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.StatusWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CrawlQueueWorker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlQueueWorker.class);

    static final RethinkDB r = RethinkDB.r;

    private final Frontier frontier;
    private final RethinkDbConnection conn;
    private final JedisPool jedisPool;
    private final ChgDelayedQueueScript delayedChgQueueScript;
    private final ChgBusyTimeoutScript chgBusyTimeoutScript;
    private final ScheduledExecutorService executor;

    Runnable chgQueueWorker = new Runnable() {
        @Override
        public void run() {
            try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
                Long moved = delayedChgQueueScript.run(ctx, CrawlQueueManager.CHG_WAIT_KEY, CrawlQueueManager.CHG_READY_KEY);
                if (moved > 0) {
                    LOG.debug("{} CrawlHostGroups moved from wait state to ready state", moved);
                }
                moved = chgBusyTimeoutScript.run(ctx);
                if (moved > 0) {
                    LOG.warn("{} CrawlHostGroups moved from busy state to ready state", moved);
                }
                moved = delayedChgQueueScript.run(ctx, CrawlQueueManager.CRAWL_EXECUTION_RUNNING_KEY, CrawlQueueManager.CRAWL_EXECUTION_TIMEOUT_KEY);
                if (moved > 0) {
                    LOG.debug("{} CrawlExecutions moved from running state to timeout state", moved);
                }
            } catch (Throwable t) {
                LOG.error("Error running chg queue manager script", t);
            }
        }
    };

    Runnable removeUriQueueWorker = new Runnable() {
        @Override
        public void run() {
            try (Jedis jedis = jedisPool.getResource()) {
                List<String> toBeRemoved = jedis.lrange(CrawlQueueManager.REMOVE_URI_QUEUE_KEY, 0, 9999);
                if (!toBeRemoved.isEmpty()) {
                    // Remove queued uris from DB
                    long deleted = conn.exec("db-deleteQueuedUri",
                            r.table(Tables.URI_QUEUE.name)
                                    .getAll(toBeRemoved.toArray())
                                    .delete().optArg("durability", "soft")
                                    .g("deleted")
                    );
                    Pipeline p = jedis.pipelined();
                    for (String uriId : toBeRemoved) {
                        p.lrem(CrawlQueueManager.REMOVE_URI_QUEUE_KEY, 1, uriId);
                    }
                    p.sync();
                    LOG.debug("Deleted {} URIs from crawl queue", deleted);
                }
            } catch (Throwable t) {
                LOG.error("Error running chg queue manager script", t);
            }
        }
    };

    Runnable crawlExecutionTimeoutWorker = new Runnable() {
        @Override
        public void run() {
            try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
                String toBeRemoved = ctx.getJedis().lpop(CrawlQueueManager.CRAWL_EXECUTION_TIMEOUT_KEY);
                while (toBeRemoved != null) {
                    try {
                        StatusWrapper s = StatusWrapper.getStatusWrapper(frontier, toBeRemoved);
                        switch (s.getState()) {
                            case SLEEPING:
                            case CREATED:
                                LOG.debug("CrawlExecution '{}' with state {} timed out", s.getId(), s.getState());
                                s.incrementDocumentsDenied(frontier.getCrawlQueueManager()
                                        .deleteQueuedUrisForExecution(ctx, toBeRemoved))
                                        .setEndState(State.ABORTED_TIMEOUT)
                                        .saveStatus();
                                break;
                            default:
                                LOG.trace("CrawlExecution '{}' with state {} was already finished", s.getId(), s.getState());
                        }
                    } catch (Exception e) {
                        // Don't worry execution will be deleted at some point later
                    }

                    toBeRemoved = ctx.getJedis().lpop(CrawlQueueManager.CRAWL_EXECUTION_TIMEOUT_KEY);
                }
            } catch (Exception e) {
                e.printStackTrace();
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
            }
        }
    };

    public CrawlQueueWorker(Frontier frontier, RethinkDbConnection conn, JedisPool jedisPool) {
        this.frontier = frontier;
        this.conn = conn;
        this.jedisPool = jedisPool;
        executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("CrawlQueueWorker-%d").build());

        delayedChgQueueScript = new ChgDelayedQueueScript();
        chgBusyTimeoutScript = new ChgBusyTimeoutScript();
        executor.scheduleWithFixedDelay(chgQueueWorker, 400, 400, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(removeUriQueueWorker, 1000, 1000, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(crawlExecutionTimeoutWorker, 1100, 1100, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(checkPaused, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(15, TimeUnit.SECONDS);
    }
}
