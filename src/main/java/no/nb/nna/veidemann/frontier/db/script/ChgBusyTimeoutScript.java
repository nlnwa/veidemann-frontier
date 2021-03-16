package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_BUSY_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.SESSION_TO_CHG_KEY;

public class ChgBusyTimeoutScript extends RedisJob<List<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgBusyTimeoutScript.class);

    final LuaScript chgBusyTimeoutScript;

    public ChgBusyTimeoutScript() {
        super("chgBusyTimeoutScript");
        chgBusyTimeoutScript = new LuaScript("chg_busy_timeout.lua");
    }

    /**
     * Move CrawlHostGroups which have timed out in busy state, to ready state.
     *
     * @param ctx Jedis context
     * @return number of CrawlHostGroups moved from busy to ready
     */
    public List<String> run(JedisContext ctx) {
        return execute(ctx, jedis -> {
            List<String> keys = ImmutableList.of(CHG_BUSY_KEY, SESSION_TO_CHG_KEY);
            List<String> args = ImmutableList.of(String.valueOf(System.currentTimeMillis()));
            return (List<String>) chgBusyTimeoutScript.runString(jedis, keys, args);
        });
    }
}
