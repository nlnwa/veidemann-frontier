/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import com.google.common.net.InetAddresses;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.util.ApiTools;

import java.math.BigInteger;
import java.util.List;

/**
 *
 */
public class CrawlHostGroupCalculator {

    private CrawlHostGroupCalculator() {
    }

    /**
     * Calculate the group this uri belongs to for politeness purpose.
     * <p>
     * Check if uri's ip is within range of a crawl host group. If it is return the groups id. Otherwise return the
     * hashed ip address.
     *
     * @param ip                    the ip for wich to calculate the group
     * @param crawlHostGroupConfigs the CrawlHostGroup configs to check
     * @return id of a CrawlHostGroup or the hashed IP
     */
    public static String calculateCrawlHostGroup(String ip, List<ConfigObject> crawlHostGroupConfigs) {
        BigInteger ipVal = ipAsInteger(ip);

        String hostGroupHash = crawlHostGroupConfigs.stream()
                .filter(g -> {
                    return g.getCrawlHostGroupConfig().getIpRangeList().stream()
                            .anyMatch(r -> inRange(ipAsInteger(r.getIpFrom()), ipAsInteger(r.getIpTo()), ipVal));
                })
                .findFirst()
                .map(g -> g.getId())
                .orElse(ApiTools.createSha1Digest(ip));

        return hostGroupHash;
    }

    private static BigInteger ipAsInteger(String ip) {
        return new BigInteger(InetAddresses.forString(ip).getAddress());
    }

    private static boolean inRange(BigInteger rangeStart, BigInteger rangeEnd, BigInteger toCheck) {
        return (toCheck.compareTo(rangeStart) >= 0 && toCheck.compareTo(rangeEnd) <= 0);
    }

}
