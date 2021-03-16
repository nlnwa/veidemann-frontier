package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_BUSY_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_PREFIX;

public class ChgQueueCountScript extends RedisJob<Long> {
    final LuaScript chgQueueCountScript;

    public ChgQueueCountScript() {
        super("chgQueueCountScript");
        chgQueueCountScript = new LuaScript("chg_queue_count.lua");
    }

    public Long run(JedisContext ctx, CrawlHostGroup chg) {
        return execute(ctx, jedis -> {
            String chgId = chg.getId();
            String chgKey = CHG_PREFIX + chgId;
            List<String> keys = ImmutableList.of(chgKey, CHG_BUSY_KEY);
            List<String> args = ImmutableList.of(chgId);
            return (Long) chgQueueCountScript.runString(jedis, keys, args);
        });
    }
}
