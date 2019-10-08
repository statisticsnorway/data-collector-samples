package no.ssb.dc.samples.ske.freg;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.node.builder.XPathBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.handler.Queries;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.regex;
import static no.ssb.dc.api.Builders.security;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

// https://skatteetaten.github.io/folkeregisteret-api-dokumentasjon/oppslag/
public class FregWorkerTest {

    @Test
    public void testXPathQuery() {
        String xml = CommonUtils.readFileOrClasspathResource("testdata/freg/entry.xml");
        System.out.printf("xml: %s%n", xml);
        XPathBuilder xPathBuilder = xpath("/entry/content/lagretHendelse/sekvensnummer");
        String queryResult = Queries.from(xPathBuilder.build()).evaluateStringLiteral(xml.getBytes());
        System.out.printf("result: '%s'%n", queryResult);
    }

    @Ignore
    @Test
    public void thatWorkerCollectSiriusData() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.store.provider", "rawdata")
                .values("rawdata.client.provider", "postgres")
                .values("data.collector.worker.threads", "50")
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "rdc")
                .values("postgres.driver.password", "rdc")
                .values("postgres.driver.database", "rdc")
                .values("postgres.recreate-database", "false")
                .values("rawdata.postgres.consumer.prefetch-size", "100")
                .values("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "1000")
                .build();

        Worker.newBuilder()
                .configuration(configuration.asMap())
                .buildCertificateFactory(CommonUtils.currentPath())
                //.stopAtNumberOfIterations(5)
                .flow(Flow.start("Collect FREG", "loop")
                        .configure(context()
                                .topic("freg")
                                .header("accept", "application/xml")
                                .variable("ProdusentTestURL", "https://folkeregisteret-api-ekstern.sits.no")
                                .variable("KonsumentTestURL", "https://folkeregisteret-api-konsument.sits.no")
                                .variable("ProduksjonURL", "https://folkeregisteret.api.skatteetaten.no")
                                .variable("PlaygroundURL", "https://folkeregisteret-api-konsument-playground.sits.no")
                                .variable("fromSequence", "1")
                        )
                        .configure(security()
                                .sslBundleName("ske-test-certs")
                        )
                        .function(paginate("loop")
                                .variable("fromSequence", "${nextSequence}")
                                .addPageContent()
                                .iterate(execute("event-list"))
                                .prefetchThreshold(150)
                                .until(whenVariableIsNull("nextSequence"))
                        )
                        .function(get("event-list")
                                .url("${PlaygroundURL}/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/feed/?seq=${fromSequence}")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .pipe(sequence(xpath("/feed/entry"))
                                        .expected(xpath("/entry/content/lagretHendelse/sekvensnummer"))
                                )
                                .pipe(nextPage()
                                        .output("nextSequence",
                                                regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]seq=)[^&]*")
                                        )
                                )
                                .pipe(parallel(xpath("/feed/entry"))
                                        .variable("position", xpath("/entry/content/lagretHendelse/sekvensnummer"))
                                        .pipe(addContent("${position}", "entry"))
                                        .pipe(execute("event-document")
                                                .inputVariable("eventId", xpath("/entry/content/lagretHendelse/hendelse/hendelsesdokument"))
                                        )
                                        .pipe(execute("person-document")
                                                .inputVariable("personId", xpath("/entry/content/lagretHendelse/hendelse/folkeregisteridentifikator"))
                                        )
                                        .pipe(publish("${position}"))
                                )
                                .returnVariables("nextSequence")
                        )
                        .function(get("event-document")
                                .url("${PlaygroundURL}/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/${eventId}")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .pipe(addContent("${position}", "event"))
                        )
                        .function(get("person-document")
                                .url("${PlaygroundURL}/folkeregisteret/offentlig-med-hjemmel/api/v1/personer/${personId}")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .pipe(addContent("${position}", "person"))
                        )
                )
                .build()
                .run();

    }
}
