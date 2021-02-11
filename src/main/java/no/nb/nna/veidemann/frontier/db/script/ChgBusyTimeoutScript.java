package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;

import java.util.List;

public class ChgBusyTimeoutScript extends RedisJob<Long> {
    // time to wait after timeout to give the Frontier a possibility to clean up
    // before doing it the hard way here
    final static long WAIT_TIME_MS = 5000;

    final LuaScript chgBusyTimeoutScript;

    public ChgBusyTimeoutScript() {
        super("chgBusyTimeoutScript");
        chgBusyTimeoutScript = new LuaScript("chg_busy_timeout.lua");
    }

    public Long run(JedisContext ctx) {
        return execute(ctx, jedis -> {
            List<String> keys = ImmutableList.of(CrawlQueueManager.CHG_BUSY_KEY, CrawlQueueManager.CHG_READY_KEY);
            List<String> args = ImmutableList.of(String.valueOf(System.currentTimeMillis() - WAIT_TIME_MS),
                    CrawlQueueManager.CHG_PREFIX);
            return (Long) chgBusyTimeoutScript.runString(jedis, keys, args);
        });
    }
}
