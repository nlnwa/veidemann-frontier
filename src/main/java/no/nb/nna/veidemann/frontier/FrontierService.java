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
package no.nb.nna.veidemann.frontier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.frontier.api.FrontierApiServer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.worker.DnsServiceClient;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.RobotsServiceClient;
import no.nb.nna.veidemann.frontier.worker.LogServiceClient;
import no.nb.nna.veidemann.frontier.worker.ScopeServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Class for launching the service.
 */
public class FrontierService {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierService.class);

    private static final Settings SETTINGS;

    public static final ExecutorService asyncFunctionsExecutor;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

//        TODO: Add tracing
//        TracerFactory.init("Frontier");

        asyncFunctionsExecutor = new ThreadPoolExecutor(2, 128, 15, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("asyncFunc-%d").build(), new CallerRunsPolicy());
    }

    /**
     * Create a new Frontier service.
     */
    public FrontierService() {
    }

    /**
     * Start the service.
     * <p>
     *
     * @return this instance
     */
    public FrontierService start() {
        DefaultExports.initialize();
        try {
            HTTPServer server = new HTTPServer(SETTINGS.getPrometheusPort());
        } catch (IOException ex) {
            System.err.println("Could not start Prometheus exporter: " + ex.getLocalizedMessage());
            System.exit(3);
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(64);
        try (DbService db = DbService.configure(SETTINGS);

             JedisPool jedisPool = new JedisPool(jedisPoolConfig, URI.create("redis://" + SETTINGS.getRedisHost() + ':' + SETTINGS.getRedisPort()));

             RobotsServiceClient robotsServiceClient = new RobotsServiceClient(
                     SETTINGS.getRobotsEvaluatorHost(), SETTINGS.getRobotsEvaluatorPort(), asyncFunctionsExecutor);

             DnsServiceClient dnsServiceClient = new DnsServiceClient(
                     SETTINGS.getDnsResolverHost(), SETTINGS.getDnsResolverPort(), asyncFunctionsExecutor);

             ScopeServiceClient scopeServiceClient = new ScopeServiceClient(
                     SETTINGS.getScopeserviceHost(), SETTINGS.getScopeservicePort());

             OutOfScopeHandlerClient outOfScopeHandlerClient = new OutOfScopeHandlerClient(
                     SETTINGS.getOutOfScopeHandlerHost(), SETTINGS.getOutOfScopeHandlerPort());

             LogServiceClient logServiceClient = new LogServiceClient(
                     SETTINGS.getLogServiceHost(), SETTINGS.getLogServicePort());

             Frontier frontier = new Frontier(SETTINGS, jedisPool, robotsServiceClient, dnsServiceClient, scopeServiceClient,
                     outOfScopeHandlerClient, logServiceClient);
        ) {

            FrontierApiServer apiServer = new FrontierApiServer(SETTINGS.getApiPort(), SETTINGS.getTerminationGracePeriodSeconds(), frontier);
            registerShutdownHook(apiServer);

            apiServer.start();

            LOG.info("Veidemann Frontier (v. {}) started", System.getenv("VERSION"));

            apiServer.blockUntilShutdown();
        } catch (ConfigException ex) {
            LOG.error("Configuration error: {}", ex.getLocalizedMessage());
            System.exit(1);
        } catch (Exception ex) {
            LOG.error("Could not start service", ex);
            System.exit(1);
        }

        return this;
    }

    private void registerShutdownHook(FrontierApiServer apiServer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            apiServer.shutdown();
        }));
    }

    /**
     * Get the settings object.
     * <p>
     *
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
