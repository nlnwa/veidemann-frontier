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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.frontier.api.FrontierApiServer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class for launching the service.
 */
public class FrontierService {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierService.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        System.setProperty("lock.redis.host", SETTINGS.getLockRedisHost());
        System.setProperty("lock.redis.port", SETTINGS.getLockRedisPort());

        TracerFactory.init("Frontier");
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

        try (DbService db = DbService.configure(SETTINGS);

             RobotsServiceClient robotsServiceClient = new RobotsServiceClient(
                     SETTINGS.getRobotsEvaluatorHost(), SETTINGS.getRobotsEvaluatorPort());

             DnsServiceClient dnsServiceClient = new DnsServiceClient(
                     SETTINGS.getDnsResolverHost(), SETTINGS.getDnsResolverPort());

             OutOfScopeHandlerClient outOfScopeHandlerClient = new OutOfScopeHandlerClient(
                     SETTINGS.getOutOfScopeHandlerHost(), SETTINGS.getOutOfScopeHandlerPort());

             Frontier frontier = new Frontier(robotsServiceClient, dnsServiceClient, outOfScopeHandlerClient);

             FrontierApiServer apiServer = new FrontierApiServer(SETTINGS.getApiPort(), SETTINGS.getTerminationGracePeriodSeconds(), frontier);) {

            registerShutdownHook();

            apiServer.start();

            LOG.info("Veidemann Frontier (v. {}) started",
                    FrontierService.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException ex) {
            LOG.error("Configuration error: {}", ex.getLocalizedMessage());
            System.exit(1);
        } catch (Exception ex) {
            LOG.error("Could not start service", ex);
            System.exit(1);
        }

        return this;
    }

    private void registerShutdownHook() {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");

            mainThread.interrupt();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                //
            }

            System.err.println("*** gracefully shut down");
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
