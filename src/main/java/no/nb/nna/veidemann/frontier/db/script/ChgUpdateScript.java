package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ChgUpdateScript extends RedisJob<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgUpdateScript.class);

    public ChgUpdateScript() {
        super("chgUpdate");
    }

    public void run(JedisContext ctx, CrawlHostGroup crawlHostGroup) {
        execute(ctx, jedis -> {
            Map<String, String> encoded = CrawlHostGroupCodec.encodeMap(crawlHostGroup);
            jedis.hset(CrawlQueueManager.CHG_PREFIX + crawlHostGroup.getId(), encoded);
            if (!crawlHostGroup.getSessionToken().isEmpty()) {
                jedis.hset(CrawlQueueManager.SESSION_TO_CHG_KEY, crawlHostGroup.getSessionToken(), crawlHostGroup.getId());
            }
            return null;
        });
    }
}
