package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_BUSY_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_WAIT_KEY;

public class ChgReleaseScript extends RedisJob<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgReleaseScript.class);

    public ChgReleaseScript(JedisPool jedisPool) {
        super(jedisPool, "release chg");
    }

    public void run(CrawlHostGroup crawlHostGroup, long nextFetchTimeMs) {
        execute(jedis -> {
            long readyTime = nextFetchTimeMs;
            if (readyTime < System.currentTimeMillis()) {
                readyTime = System.currentTimeMillis() + 10;
            }

            String chgp = CrawlQueueManager.createChgPolitenessKey(crawlHostGroup);
            String chgpKey = CrawlQueueManager.CHG_PREFIX + chgp;

            if (!jedis.exists(chgpKey)) {
                return null;
            }
            jedis.zrem(CHG_BUSY_KEY, chgp);
            jedis.zadd(CHG_WAIT_KEY, readyTime, chgp);
            return null;
        });
    }
}
