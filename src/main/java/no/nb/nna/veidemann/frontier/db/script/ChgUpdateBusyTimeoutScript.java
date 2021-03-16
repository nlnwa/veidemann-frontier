package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;

import java.util.List;

public class ChgUpdateBusyTimeoutScript extends RedisJob<Long> {
    final LuaScript chgUpdateBusyTimeoutScript;

    public ChgUpdateBusyTimeoutScript() {
        super("chgUpdateBusyTimeoutScript");
        chgUpdateBusyTimeoutScript = new LuaScript("chg_update_busy_timeout.lua");
    }

    public Long run(JedisContext ctx, String crawlHostGroupId, String sessionToken, Long timeoutTimeMs) {
        String chgKey = CrawlQueueManager.CHG_PREFIX + crawlHostGroupId;

        return execute(ctx, jedis -> {
            List<String> keys = ImmutableList.of(CrawlQueueManager.CHG_BUSY_KEY, chgKey);
            List<String> args = ImmutableList.of(String.valueOf(timeoutTimeMs), crawlHostGroupId, sessionToken);
            Long res = (Long) chgUpdateBusyTimeoutScript.runString(jedis, keys, args);
            return res;
        });
    }
}
