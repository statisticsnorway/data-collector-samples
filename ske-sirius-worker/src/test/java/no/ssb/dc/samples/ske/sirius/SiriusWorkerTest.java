package no.ssb.dc.samples.ske.sirius;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.core.executor.BufferedReordering;
import no.ssb.dc.core.executor.FixedThreadPool;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.handler.ParallelHandler;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import static org.testng.Assert.assertNotNull;

@Listeners(TestServerListener.class)
public class SiriusWorkerTest {

    private static final Logger LOG = LoggerFactory.getLogger(SiriusWorkerTest.class);

    @Inject
    TestServer testServer;

    Services services;

    DynamicConfiguration configuration;

    ContentStore contentStore;

    @BeforeMethod
    public void setup() {
        Client.Builder clientBuilder = Client.newClientBuilder();

        SSLContext sslContext = CertsHelper.createSSLContext();
        assertNotNull(sslContext);

        clientBuilder.sslContext(sslContext);

        configuration = testServer.getConfiguration();
        contentStore = ProviderConfigurator.configure(configuration.asMap(),
                configuration.evaluateToString("content.store.provider"), ContentStoreInitializer.class);
        services = Services.create()
                .register(ConfigurationMap.class, new ConfigurationMap(configuration.asMap()))
                .register(Client.class, clientBuilder.build())
                .register(BufferedReordering.class, new BufferedReordering<>())
                .register(ContentStore.class, contentStore)
                .register(FixedThreadPool.class, FixedThreadPool.newInstance(configuration));
    }

    @Test
    public void testPrintConfig() {
        LOG.info("Config:\n{}", SiriusFlow.get().serialize());
//        LOG.info("Config:\n{}", SiriusFlow.getFlow().end().startNode());
    }

    @Ignore
    @Test
    public void thatWorkerCollectSiriusFlow() throws InterruptedException {
        ExecutionContext context = new ExecutionContext.Builder().services(services).build();

        context.state(ParallelHandler.MAX_NUMBER_OF_ITERATIONS, 1);

        String lastPosition = contentStore.lastPosition(configuration.evaluateToString("namespace.default"));
        String startPosition = (lastPosition == null ? "1" : lastPosition);
        context.variable("fromSequence", startPosition);

        Flow flow = SiriusFlow.get().end();

        Worker worker = new Worker(flow.startNode(), context);
        worker.run();

//        Worker.newBuilder()
//                .flow(SiriusFlow.get())
//                .services(services)
//                .initialPosition("1")
//                .header("Accept", "application/xml")
//                .variable("baseURL", "https://api-at.sits.no")
//                .build()
//                .run();
    }
}
