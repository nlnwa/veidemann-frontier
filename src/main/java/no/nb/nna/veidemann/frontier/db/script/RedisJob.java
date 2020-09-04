package no.nb.nna.veidemann.frontier.db.script;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class RedisJob<R extends Object> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisJob.class);

    final String name;
    final JedisPool jedisPool;
    AtomicLong runTime = new AtomicLong();
    AtomicLong invocations = new AtomicLong();
    int maxAttempts = 10;

    public RedisJob(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.name = name;
    }

    protected R execute(Function<Jedis, R> job) {
        int attempts = 0;

        while (true) {
            try (Jedis jedis = jedisPool.getResource()) {
                long start = System.nanoTime();

                R result = job.apply(jedis);

                if (LOG.isDebugEnabled()) {
                    runTime.addAndGet(System.nanoTime() - start);
                    if (invocations.incrementAndGet() % 200 == 0) {
                        LOG.debug("Script: {}, invocations: {}, avg: {}ms", name, invocations.get(), (runTime.get() / invocations.get()) / 1000000f);
                    }
                }
                return result;
            } catch (JedisConnectionException ex) {
                attempts++;
                if (attempts <= maxAttempts) {
                    LOG.warn("Failed connecting to Redis. Will retry in one second", ex);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        ex.addSuppressed(e);
                        throw ex;
                    }
                } else {
                    LOG.error("Failed connecting to Redis. Giving up after {} attempts", attempts, ex);
                    throw ex;
                }
            }
        }
    }
}
