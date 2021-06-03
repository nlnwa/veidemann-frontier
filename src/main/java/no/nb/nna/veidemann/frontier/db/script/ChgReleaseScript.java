package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgReleaseScript extends RedisJob<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgReleaseScript.class);
    final LuaScript chgRealeaseScript;

    public ChgReleaseScript() {
        super("chgRealeaseScript");
        chgRealeaseScript = new LuaScript("chg_release.lua");
    }

    public Long run(JedisContext ctx, String crawlHostGroupId, String sessionToken, long nextFetchDelayMs) {
        if (nextFetchDelayMs < 10) {
            nextFetchDelayMs = 10;
        }
        String chgKey = CrawlQueueManager.CHG_PREFIX + crawlHostGroupId;

        long waitTime = System.currentTimeMillis() + nextFetchDelayMs;

        List<String> keys = ImmutableList.of(CHG_BUSY_KEY, CHG_WAIT_KEY, chgKey, SESSION_TO_CHG_KEY);
        List<String> args = ImmutableList.of(String.valueOf(waitTime), crawlHostGroupId, sessionToken);

        return execute(ctx, jedis -> {
            try {
                String result = (String) chgRealeaseScript.runString(jedis, keys, args);
                return Long.parseLong(result);
            } catch (JedisDataException e) {
                LOG.warn(e.getMessage());
                return 0L;
            }
        });
    }
}
