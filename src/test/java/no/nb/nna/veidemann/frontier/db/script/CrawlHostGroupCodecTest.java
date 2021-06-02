package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CrawlHostGroupCodecTest {

    @Test
    void encodeList() {
        CrawlHostGroup chg = CrawlHostGroup.newBuilder().build();
        List<String> result = CrawlHostGroupCodec.encodeList(chg);
        List<String> expectedResult = ImmutableList.of("mi", "0", "ma", "0", "df", "0.0", "mr", "0", "rd", "0", "u", "", "st", "", "ts", "0");
        assertThat(result).isEqualTo(expectedResult);

        chg = CrawlHostGroup.newBuilder()
                .setId("id1")
                .setMaxTimeBetweenPageLoadMs(3000)
                .setMinTimeBetweenPageLoadMs(1000)
                .setMaxRetries(3)
                .setDelayFactor(1.5f)
                .setRetryDelaySeconds(60)
                .setCurrentUriId("uri1")
                .setQueuedUriCount(45)
                .setSessionToken("token")
                .setFetchStartTimeStamp(Timestamps.fromMillis(5))
                .build();
        result = CrawlHostGroupCodec.encodeList(chg);
        expectedResult = ImmutableList.of("mi", "1000", "ma", "3000", "df", "1.5", "mr", "3", "rd", "60", "u", "uri1", "st", "token", "ts", "5");
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void decodeList() {
        List<String> encoded = new ArrayList<>();
        CrawlHostGroup result = CrawlHostGroupCodec.decode("id1", encoded);
        CrawlHostGroup expectedResult = CrawlHostGroup.newBuilder().setId("id1").build();
        assertThat(result).isEqualTo(expectedResult);

        encoded = ImmutableList.of("mi", "0", "ma", "0", "df", "0.0", "mr", "0", "rd", "0", "qc", "0", "u", "", "st", "", "ts", "0");
        result = CrawlHostGroupCodec.decode("id1", encoded);
        expectedResult = CrawlHostGroup.newBuilder().setId("id1").build();
        assertThat(result).isEqualTo(expectedResult);

        encoded = ImmutableList.of("mi", "1000", "ma", "3000", "df", "1.5", "mr", "3", "rd", "60", "qc", "45", "u", "uri1", "st", "token", "ts", "5");
        expectedResult = CrawlHostGroup.newBuilder()
                .setId("id1")
                .setMaxTimeBetweenPageLoadMs(3000)
                .setMinTimeBetweenPageLoadMs(1000)
                .setMaxRetries(3)
                .setDelayFactor(1.5f)
                .setRetryDelaySeconds(60)
                .setCurrentUriId("uri1")
                .setQueuedUriCount(45)
                .setSessionToken("token")
                .setFetchStartTimeStamp(Timestamps.fromMillis(5))
                .build();
        result = CrawlHostGroupCodec.decode("id1", encoded);
        assertThat(result).isEqualTo(expectedResult);

        encoded = ImmutableList.of("unknown1", "34", "mi", "1000", "ma", "3000", "unknown2", "df", "1.5", "mr", "3", "rd", "60", "qc", "45", "u", "uri1", "st", "token", "ts", "5");
        result = CrawlHostGroupCodec.decode("id1", encoded);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void encodeMap() {
        CrawlHostGroup chg = CrawlHostGroup.newBuilder().build();
        Map<String, String> result = CrawlHostGroupCodec.encodeMap(chg);
        Map<String, String> expectedResult = new ImmutableMap.Builder<String, String>()
                .put("mi", "0")
                .put("ma", "0")
                .put("df", "0.0")
                .put("mr", "0")
                .put("rd", "0")
                .put("u", "")
                .put("st", "")
                .put("ts", "0")
                .build();
        assertThat(result).isEqualTo(expectedResult);

        chg = CrawlHostGroup.newBuilder()
                .setId("id1")
                .setMaxTimeBetweenPageLoadMs(3000)
                .setMinTimeBetweenPageLoadMs(1000)
                .setMaxRetries(3)
                .setDelayFactor(1.5f)
                .setRetryDelaySeconds(60)
                .setCurrentUriId("uri1")
                .setQueuedUriCount(45)
                .setSessionToken("token")
                .setFetchStartTimeStamp(Timestamps.fromMillis(5))
                .build();
        result = CrawlHostGroupCodec.encodeMap(chg);
        expectedResult = new ImmutableMap.Builder<String, String>()
                .put("mi", "1000")
                .put("ma", "3000")
                .put("df", "1.5")
                .put("mr", "3")
                .put("rd", "60")
                .put("u", "uri1")
                .put("st", "token")
                .put("ts", "5")
                .build();
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void decodeMap() {
        Map<String, String> encoded = ImmutableMap.of();
        CrawlHostGroup result = CrawlHostGroupCodec.decode("id1", encoded);
        CrawlHostGroup expectedResult = CrawlHostGroup.newBuilder().setId("id1").build();
        assertThat(result).isEqualTo(expectedResult);

        encoded = new ImmutableMap.Builder<String, String>()
                .put("mi", "0")
                .put("ma", "0")
                .put("df", "0.0")
                .put("mr", "0")
                .put("rd", "0")
                .put("qc", "0")
                .put("u", "")
                .put("st", "")
                .put("ts", "0")
                .build();
        result = CrawlHostGroupCodec.decode("id1", encoded);
        expectedResult = CrawlHostGroup.newBuilder().setId("id1").build();
        assertThat(result).isEqualTo(expectedResult);

        encoded = new ImmutableMap.Builder<String, String>()
                .put("mi", "1000")
                .put("ma", "3000")
                .put("df", "1.5")
                .put("mr", "3")
                .put("rd", "60")
                .put("qc", "45")
                .put("u", "uri1")
                .put("st", "token")
                .put("ts", "5")
                .build();
        expectedResult = CrawlHostGroup.newBuilder()
                .setId("id1")
                .setMaxTimeBetweenPageLoadMs(3000)
                .setMinTimeBetweenPageLoadMs(1000)
                .setMaxRetries(3)
                .setDelayFactor(1.5f)
                .setRetryDelaySeconds(60)
                .setCurrentUriId("uri1")
                .setQueuedUriCount(45)
                .setSessionToken("token")
                .setFetchStartTimeStamp(Timestamps.fromMillis(5))
                .build();
        result = CrawlHostGroupCodec.decode("id1", encoded);
        assertThat(result).isEqualTo(expectedResult);

        encoded = new ImmutableMap.Builder<String, String>()
                .put("unknown1", "34")
                .put("mi", "1000")
                .put("ma", "3000")
                .put("df", "1.5")
                .put("mr", "3")
                .put("rd", "60")
                .put("qc", "45")
                .put("u", "uri1")
                .put("st", "token")
                .put("ts", "5")
                .build();
        result = CrawlHostGroupCodec.decode("id1", encoded);
        assertThat(result).isEqualTo(expectedResult);
    }
}