package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgReleaseScript extends RedisJob<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgReleaseScript.class);
    final LuaScript chgRealeaseScript;

    public ChgReleaseScript() {
        super("chgRealeaseScript");
        chgRealeaseScript = new LuaScript("chg_release.lua");
    }

    public void run(JedisContext ctx, String crawlHostGroupId, String sessionToken, long nextFetchDelayMs) {
        if (nextFetchDelayMs <= 0) {
            nextFetchDelayMs = 10;
        }
        String chgKey = CrawlQueueManager.CHG_PREFIX + crawlHostGroupId;

        long waitTime = System.currentTimeMillis() + nextFetchDelayMs;

        List<String> keys = ImmutableList.of(CHG_BUSY_KEY, CHG_WAIT_KEY, chgKey, SESSION_TO_CHG_KEY);
        List<String> args = ImmutableList.of(String.valueOf(waitTime), crawlHostGroupId, sessionToken);

        execute(ctx, jedis -> {
            try {
                chgRealeaseScript.runString(jedis, keys, args);
            } catch (JedisDataException e) {
                LOG.warn(e.getMessage());
            }
            return null;
        });
    }
}
