package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;

import java.util.List;

public class ChgBusyTimeoutScript extends RedisJob<Long> {
    final LuaScript chgBusyTimeoutScript;

    public ChgBusyTimeoutScript() {
        super("chgBusyTimeoutScript");
        chgBusyTimeoutScript = new LuaScript("chg_busy_timeout.lua");
    }

    public Long run(JedisContext ctx) {
        return execute(ctx, jedis -> {
            List<String> keys = ImmutableList.of(CrawlQueueManager.CHG_BUSY_KEY, CrawlQueueManager.CHG_READY_KEY);
            List<String> args = ImmutableList.of(String.valueOf(System.currentTimeMillis()), CrawlQueueManager.CHG_PREFIX);
            return (Long) chgBusyTimeoutScript.runString(jedis, keys, args);
        });
    }
}
