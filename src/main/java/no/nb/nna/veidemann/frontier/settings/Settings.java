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
package no.nb.nna.veidemann.frontier.settings;

import no.nb.nna.veidemann.commons.settings.CommonSettings;

import java.time.Duration;

/**
 * Configuration settings for Veidemann Frontier.
 */
public class Settings extends CommonSettings {

    private int apiPort;

    private String workDir;

    private String robotsEvaluatorHost;

    private int robotsEvaluatorPort;

    private String dnsResolverHost;

    private int dnsResolverPort;

    private String scopeserviceHost;

    private int scopeservicePort;

    private String outOfScopeHandlerHost;

    private int outOfScopeHandlerPort;

    private int prometheusPort;

    private int terminationGracePeriodSeconds;

    private String redisHost;

    private String redisPort;

    private Duration busyTimeout;

    public Duration getBusyTimeout() {
        return busyTimeout;
    }

    public void setBusyTimeout(Duration busyTimeout) {
        this.busyTimeout = busyTimeout;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public String getRobotsEvaluatorHost() {
        return robotsEvaluatorHost;
    }

    public void setRobotsEvaluatorHost(String robotsEvaluatorHost) {
        this.robotsEvaluatorHost = robotsEvaluatorHost;
    }

    public int getRobotsEvaluatorPort() {
        return robotsEvaluatorPort;
    }

    public void setRobotsEvaluatorPort(int robotsEvaluatorPort) {
        this.robotsEvaluatorPort = robotsEvaluatorPort;
    }

    public String getDnsResolverHost() {
        return dnsResolverHost;
    }

    public void setDnsResolverHost(String dnsResolverHost) {
        this.dnsResolverHost = dnsResolverHost;
    }

    public int getDnsResolverPort() {
        return dnsResolverPort;
    }

    public void setDnsResolverPort(int dnsResolverPort) {
        this.dnsResolverPort = dnsResolverPort;
    }

    public String getScopeserviceHost() {
        return scopeserviceHost;
    }

    public void setScopeserviceHost(String scopeserviceHost) {
        this.scopeserviceHost = scopeserviceHost;
    }

    public int getScopeservicePort() {
        return scopeservicePort;
    }

    public void setScopeservicePort(int scopeservicePort) {
        this.scopeservicePort = scopeservicePort;
    }

    public String getOutOfScopeHandlerHost() {
        return outOfScopeHandlerHost;
    }

    public void setOutOfScopeHandlerHost(String outOfScopeHandlerHost) {
        this.outOfScopeHandlerHost = outOfScopeHandlerHost;
    }

    public int getOutOfScopeHandlerPort() {
        return outOfScopeHandlerPort;
    }

    public void setOutOfScopeHandlerPort(int outOfScopeHandlerPort) {
        this.outOfScopeHandlerPort = outOfScopeHandlerPort;
    }

    public int getPrometheusPort() {
        return prometheusPort;
    }

    public void setPrometheusPort(int prometheusPort) {
        this.prometheusPort = prometheusPort;
    }

    public int getTerminationGracePeriodSeconds() {
        return terminationGracePeriodSeconds;
    }

    public void setTerminationGracePeriodSeconds(int terminationGracePeriodSeconds) {
        this.terminationGracePeriodSeconds = terminationGracePeriodSeconds;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public String getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(String redisPort) {
        this.redisPort = redisPort;
    }
}
