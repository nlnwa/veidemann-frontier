package no.nb.nna.veidemann.frontier.api;

import io.opentracing.mock.MockSpan;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.RunningCrawl;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.SeedAndExecutions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

@Testcontainers
@Tag("integration")
@Tag("redis")
@Tag("rethinkDb")
public class TracerTest extends no.nb.nna.veidemann.frontier.testutil.AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(TracerTest.class);

    /**
     * For now this test only writes the traces to the console. No real checks yet.
     *
     * @throws Exception
     */
    @Test
    public void testOneSuccessfullSeed() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 2;
        int maxHopsFromSeed = 1;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        List<MockSpan> finishedSpans = tracer.finishedSpans();
        class item implements Comparable<item> {
            Long id;
            MockSpan span;
            SortedSet<item> children = new TreeSet<>();

            public item(MockSpan span) {
                this.id = span.context().spanId();
                this.span = span;
            }

            @Override
            public String toString() {
                return toString("");
            }

            public String toString(String indent) {
                StringBuilder sb = new StringBuilder(indent + span.operationName() + "(" + span.context().spanId() + ")")
                        .append(", parent=" + span.parentId()).append('\n');
                sb.append(indent).append("    tags: ").append(span.tags()).append('\n');
                span.logEntries().forEach(l -> sb.append(indent).append("    * log: ").append(l.fields()).append('\n'));
                children.forEach(c -> sb.append(c.toString(indent + "  ")));
                return sb.toString();
            }

            @Override
            public int compareTo(@NotNull item o) {
                return (int) (span.startMicros() - o.span.startMicros());
            }
        }

        Map<Long, item> spantree = new HashMap<>();

        finishedSpans.stream().map(s -> new item(s)).forEach(i -> spantree.put(i.id, i));
        spantree.forEach((k, v) -> {
            if (v.span.parentId() > 0) {
                try {
                    spantree.get(v.span.parentId()).children.add(v);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        spantree.forEach((k, v) -> {
            if (v.span.parentId() == 0) System.out.println(v);
        });
    }
}
