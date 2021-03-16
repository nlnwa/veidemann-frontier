package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.script.CrawlHostGroupCodec;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class RedisData {
    final JedisPool jedisPool;

    public RedisData(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public long getQueueCountTotal() {
        try (Jedis jedis = jedisPool.getResource()) {
            String val = jedis.get("QCT");
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public Map<String, Long> getCrawlExecutionCounts() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll("EIDC").entrySet().stream()
                    .collect(Collectors.toUnmodifiableMap(e -> e.getKey(), e -> Long.parseLong(e.getValue())));
        }
    }

    public Map<String, CrawlHostGroup> getCrawlHostGroups() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(CHG_PREFIX + "*").stream()
                    .map(k -> CrawlHostGroupCodec.decode(k.substring(CHG_PREFIX.length()), jedis.hgetAll(k)))
                    .collect(Collectors.toUnmodifiableMap(chg -> chg.getId(), chg -> chg));
        }
    }

    public Set<Tuple> getWaitQueue() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zrangeWithScores(CHG_WAIT_KEY, 0, -1);
        }
    }

    public Set<Tuple> getBusyQueue() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zrangeWithScores(CHG_BUSY_KEY, 0, -1);
        }
    }

    public List<String> getReadyQueue() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(CHG_READY_KEY, 0, -1);
        }
    }

    public Map<String, String> getSessionTokens() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(SESSION_TO_CHG_KEY);
        }
    }
}
