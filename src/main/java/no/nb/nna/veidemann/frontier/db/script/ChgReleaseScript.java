package no.nb.nna.veidemann.frontier.db.script;

import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgReleaseScript extends RedisJob<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgReleaseScript.class);

    public ChgReleaseScript(JedisPool jedisPool) {
        super(jedisPool, "release chg");
    }

    public void run(CrawlHostGroup crawlHostGroup) {
        execute(jedis -> {
            CrawlHostGroup chg = crawlHostGroup.toBuilder().setBusy(false).build();
            long readyTime = Timestamps.toMillis(chg.getNextFetchTime());
            long sysTime = System.currentTimeMillis();
            if (readyTime <= sysTime) {
                readyTime = 0;
            }

            String chgp = CrawlQueueManager.createChgPolitenessKey(chg);
            String chgpKey = CrawlQueueManager.CHG_PREFIX + chgp;

            if (!jedis.exists(chgpKey)) {
                return null;
            }
            jedis.zrem(CHG_BUSY_KEY, chgp);
            jedis.hset(chgpKey.getBytes(), CrawlQueueManager.serializeCrawlHostGroup(chg));
            if (readyTime > System.currentTimeMillis()) {
                jedis.zadd(CHG_WAIT_KEY, readyTime, chgp);
            } else {
                jedis.rpush(CHG_READY_KEY, chgp);
            }
            return null;
        });
    }
}
