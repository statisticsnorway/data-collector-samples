package no.ssb.dc.samples.toll.tvinn;

import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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

public class TvinnWorkerTest {

    static final SpecificationBuilder specificationBuilder = Specification.start("TOLL-TVINN","Collect Tvinn", "loop")
            .configure(context()
                    .topic("tvinn-test")
                    .header("Content-Type", "application/xml")
//                    .variable("TestURL", "https://mp-at.toll.no")
                    .variable("TestURL", "https://api-test.toll.no")
                    .variable("ProduksjonURL", "https://mp-sit.toll.no")
                    .variable("nextMarker", "${contentStream.lastOrInitialPosition(\"last\")}")
            )
            .configure(security()
                    .sslBundleName("toll-test-certs")
            )
            .function(paginate("loop")
                    .variable("fromMarker", "${nextMarker}")
                    .addPageContent()
                    .iterate(execute("event-list"))
                    .prefetchThreshold(150)
                    .until(whenVariableIsNull("nextMarker"))
            )
            .function(get("event-list")
//                    .url("${TestURL}/atomfeed/toll/deklarasjon-ekstern-feed/?marker=${fromMarker}&limit=25&direction=forward")
                    .url("${TestURL}/api/declaration/declaration-clearance-feed/atom?marker=${fromMarker}&limit=25&direction=forward")
                    .validate(status().success(200))
                    .pipe(sequence(xpath("/feed/entry"))
                            .expected(xpath("/entry/id"))
                    )
                    .pipe(nextPage()
                            .output("nextMarker",
                                    regex(xpath("/feed/link[@rel=\"previous\"]/@href"), "(?<=[?&]marker=)[^&]*")
                            )
                    )
                    .pipe(parallel(xpath("/feed/entry"))
                            .variable("position", xpath("/entry/id"))
                            .pipe(addContent("${position}", "entry"))
                            .pipe(publish("${position}"))
                    )
                    .returnVariables("nextMarker")
            );

    @Ignore
    @Test
    public void testCollectTvinn() {
        Worker.newBuilder()
                .configuration(new StoreBasedDynamicConfiguration.Builder()
                        .values("content.stream.connector", "discarding")
                        .values("rawdata.client.provider", "memory")
                        .values("data.collector.worker.threads", "25")
                        .values("postgres.driver.host", "localhost")
                        .values("postgres.driver.port", "5432")
                        .values("postgres.driver.user", "rdc")
                        .values("postgres.driver.password", "rdc")
                        .values("postgres.driver.database", "rdc")
                        .values("rawdata.postgres.consumer.prefetch-size", "100")
                        .values("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "1000")
                        .build()
                        .asMap())
                .buildCertificateFactory(CommonUtils.currentPath())
                //.stopAtNumberOfIterations(5)
                .printConfiguration()
                .specification(specificationBuilder)
                .build()
                .run();
    }

    @Ignore
    @Test
    public void writeTargetConsumerSpec() throws IOException {
        Path currentPath = CommonUtils.currentPath();
        Path targetPath = currentPath.resolve("data-collection-consumer-specifications");

        boolean targetProjectExists = targetPath.toFile().exists();
        if (!targetProjectExists) {
            throw new RuntimeException(String.format("Couldn't locate '%s' under currentPath: %s%n", targetPath.toFile().getName(), currentPath.toAbsolutePath().toString()));
        }

        Files.writeString(targetPath.resolve("specs").resolve("toll-tvinn-test-spec.json"), specificationBuilder.serialize());
    }
}
