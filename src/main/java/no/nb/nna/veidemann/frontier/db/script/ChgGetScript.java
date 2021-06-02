package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ChgGetScript extends RedisJob<CrawlHostGroup> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgGetScript.class);

    public ChgGetScript() {
        super("chgGetScript");
    }

    public CrawlHostGroup run(JedisContext ctx, String crawlHostGroupId) {
        return execute(ctx, jedis -> {
            Map<String, String> encoded = jedis.hgetAll(CrawlQueueManager.CHG_PREFIX + crawlHostGroupId);
            LOG.trace("HGETALL {}, RESULT: {}", CrawlQueueManager.CHG_PREFIX + crawlHostGroupId, encoded);
            return CrawlHostGroupCodec.decode(crawlHostGroupId, encoded);
        });
    }
}
