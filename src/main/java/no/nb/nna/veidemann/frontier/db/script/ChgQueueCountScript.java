package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgQueueCountScript extends RedisJob<Long> {
    final LuaScript chgQueueCountScript;

    public ChgQueueCountScript() {
        super("chgQueueCount");
        chgQueueCountScript = new LuaScript("chg_queue_count.lua");
    }

    public Long run(JedisContext ctx, CrawlHostGroup chg) {
        return execute(ctx, jedis -> {
            String chgp = createChgPolitenessKey(chg);
            String chgpKey = CHG_PREFIX + chgp;
            List<String> keys = ImmutableList.of(chgpKey, CHG_BUSY_KEY);
            List<String> args = ImmutableList.of(chgp);
            return (Long) chgQueueCountScript.runString(jedis, keys, args);
        });
    }
}
