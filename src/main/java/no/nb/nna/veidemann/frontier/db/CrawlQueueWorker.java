package no.nb.nna.veidemann.frontier.db;

import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.frontier.db.script.ChgDelayedQueueScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CrawlQueueWorker {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlQueueWorker.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;
    private final JedisPool jedisPool;
    ChgDelayedQueueScript delayedChgQueueScript;
    ScheduledExecutorService executor;

    Runnable chgQueueWorker = new Runnable() {
        @Override
        public void run() {
            try {
                Long moved = delayedChgQueueScript.run(CrawlQueueManager.CHG_WAIT_KEY, CrawlQueueManager.CHG_READY_KEY);
                if (moved > 0) {
                    LOG.debug("{} CrawlHostGroups moved from wait state to ready state", moved);
                }
                moved = delayedChgQueueScript.run(CrawlQueueManager.CHG_BUSY_KEY, CrawlQueueManager.CHG_READY_KEY);
                if (moved > 0) {
                    LOG.warn("{} CrawlHostGroups moved from busy state to ready state", moved);
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
                List<String> toBeRemoved = jedis.lrange(CrawlQueueManager.REMOVE_URI_QUEUE_KEY, 0, 4999);
                if (!toBeRemoved.isEmpty()) {
                    // Remove queued uris from DB
                    long deleted = conn.exec("db-deleteQueuedUri",
                            r.table(Tables.URI_QUEUE.name)
                                    .getAll(toBeRemoved.toArray())
                                    .delete().g("deleted")
                    );
                    for (String uriId : toBeRemoved) {
                        jedis.lrem(CrawlQueueManager.REMOVE_URI_QUEUE_KEY, 1, uriId);
                    }
                    LOG.debug("Deleted {} URIs from crawl queue", deleted);
                }
            } catch (Throwable t) {
                LOG.error("Error running chg queue manager script", t);
            }
        }
    };

    public CrawlQueueWorker(RethinkDbConnection conn, JedisPool jedisPool) {
        this.conn = conn;
        this.jedisPool = jedisPool;
        executor = Executors.newScheduledThreadPool(1);

        delayedChgQueueScript = new ChgDelayedQueueScript(jedisPool);
        executor.scheduleWithFixedDelay(chgQueueWorker, 400, 400, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(removeUriQueueWorker, 1000, 1000, TimeUnit.MILLISECONDS);
    }

}
