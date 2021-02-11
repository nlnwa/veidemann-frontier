package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;

import java.util.List;

public class ChgUpdateBusyTimeoutScript extends RedisJob<Long> {
    final static long WAIT_TIME_MS = 5000;

    final LuaScript updateChgBusyTimeoutScript;

    public ChgUpdateBusyTimeoutScript() {
        super("chgUpdateBusyTimeoutScript");
        updateChgBusyTimeoutScript = new LuaScript("chg_update_busy_timeout.lua");
    }

    public Long run(JedisContext ctx, String chg, Long timeoutTimeMs) {
        return execute(ctx, jedis -> {
            List<String> keys = ImmutableList.of(CrawlQueueManager.CHG_BUSY_KEY);
            List<String> args = ImmutableList.of(String.valueOf(timeoutTimeMs), chg);
            String res = (String) updateChgBusyTimeoutScript.runString(jedis, keys, args);
            if (res == null) {
                return null;
            }
            return Long.valueOf(res);
        });
    }
}
