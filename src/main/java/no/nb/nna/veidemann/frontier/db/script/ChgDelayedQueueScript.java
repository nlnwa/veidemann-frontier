package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class ChgDelayedQueueScript extends RedisJob<Long> {
    final LuaScript chgDealyedQueueScript;

    public ChgDelayedQueueScript(JedisPool jedisPool) {
        super(jedisPool, "chgDealyedQueue");
        chgDealyedQueueScript = new LuaScript("chg_delayed_queue.lua");
    }

    public Long run(String fromQueue, String toQueue) {
        return execute(jedis -> {
            List<String> keys = ImmutableList.of(fromQueue, toQueue);
            List<String> args = ImmutableList.of(String.valueOf(System.currentTimeMillis()));
            return (Long) chgDealyedQueueScript.runString(jedis, keys, args);
        });
    }
}
