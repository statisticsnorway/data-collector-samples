package no.ssb.dc.samples.toll.tvinn;

import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static no.ssb.dc.api.Builders.*;

public class TvinnProdWorkerTest {

    static final SpecificationBuilder specificationBuilder = Specification.start("TOLL-TVINN-PROD","Collect Tvinn Prod", "maskinporten-jwt-grant")
            .configure(context()
                            .topic("tvinn-prod")
                    .variable("ProduksjonURL", "https://api.toll.no")
                            .variable("clientId", "fedbc36a-17b8-44fd-b86b-24e0c1c161e0")
                            .variable("jwtGrantTimeToLiveInSeconds", "30")
                            .variable("nextMarker", "${contentStream.lastOrInitialPosition(\"last\")}")
            )
            .configure(security()
                            .identity(jwt("maskinporten",
                                    headerClaims()
                                            .alg("RS256")
                                            .x509CertChain("ssb-prod-certs"),
                                    claims()
                                            .audience("https://maskinporten.no/")
                                            .issuer("fedbc36a-17b8-44fd-b86b-24e0c1c161e0")
//                                            .claim("resource", "https://api-test.toll.no/api/declaration/declaration-clearance-feed/atom")
                                            .claim("scope", "toll:declaration/clearance/feed.read")
                                            .timeToLiveInSeconds("30")
                                    )
                            )
            )
            .function(post("maskinporten-jwt-grant")
                            .url("https://maskinporten.no/token/v1/token")
                            .data(bodyPublisher()
                                    .urlEncoded(jwtToken()
                                            .identityId("maskinporten")
                                            .bindTo("JWT_GRANT")
                                            .token("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${JWT_GRANT}")
                                    )
                            )
                            .validate(status().success(200))
                            .pipe(execute("loop")
//                            .pipe(execute("loop")
                                            .inputVariable("accessToken", jqpath(".access_token"))
                            )
            )
            .function(paginate("loop")
                    .variable("fromMarker", "${nextMarker}")
                    .addPageContent("fromMarker")
                    .iterate(execute("event-list").requiredInput("accessToken"))
                    .prefetchThreshold(150)
                    .until(whenVariableIsNull("nextMarker"))
            )
            .function(get("event-list")
                            .url("${ProduksjonURL}/api/declaration/declaration-clearance-feed/atom?marker=${fromMarker}&limit=10&direction=forward")
                            .header("Content-Type", "application/xml")
                            .header("Authorization", "Bearer ${accessToken}")
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
                                    .pipe(console())
                            )
                            .returnVariables("nextMarker")
            );

    @Disabled
    @Test
    public void testCollectTvinn() {
        Worker.newBuilder()
                .configuration(new StoreBasedDynamicConfiguration.Builder()
                        .values("content.stream.connector", "rawdata")
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
//                .buildCertificateFactory(CommonUtils.currentPath())
                .buildCertificateFactory(Paths.get("/Volumes/SSB BusinessSSL/certs"))
                //.stopAtNumberOfIterations(5)
                .printConfiguration()
                .specification(specificationBuilder)
                .build()
                .run();
    }

    @Disabled
    @Test
    public void writeTargetConsumerSpec() throws IOException {
        Path currentPath = CommonUtils.currentPath().getParent().getParent();
        Path targetPath = currentPath.resolve("data-collection-consumer-specifications");

        boolean targetProjectExists = targetPath.toFile().exists();
        if (!targetProjectExists) {
            throw new RuntimeException(String.format("Couldn't locate '%s' under currentPath: %s%n", targetPath.toFile().getName(), currentPath.toAbsolutePath().toString()));
        }

        Files.writeString(targetPath.resolve("specs").resolve("toll-tvinn-prod-spec.json"), specificationBuilder.serialize());
    }
}
