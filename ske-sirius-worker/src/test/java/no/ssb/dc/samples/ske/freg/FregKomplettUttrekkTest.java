package no.ssb.dc.samples.ske.freg;

import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static no.ssb.dc.api.Builders.*;

public class FregKomplettUttrekkTest {

    static final SpecificationBuilder specificationBuilder = Specification.start("SKE-FREG-UTTREKK-BATCH", "Collect FREG", "loop")
            .configure(context()
                    .topic("freg-uttrekk-komplett")
                    .variable("ProduksjonURL", "https://folkeregisteret.api.skatteetaten.no/folkeregisteret/offentlig-med-hjemmel")
                    .variable("fromFeedSequence", "0")
                    .variable("jobId", "c5d3af2f-4ac7-4506-af67-61e71c5f0b8f")
                    .variable("nextBatch", "111")
            )
            .configure(security()
                    .sslBundleName("ske-prod-certs")
            )
            .function(get("create-job")
                    .url("${ProduksjonURL}/api/v1/uttrekk/komplett?feedsekvensnr=${fromFeedSequence}")
                    .validate(status().success(200))
                    .pipe(execute("loop")
                            .requiredInput("jobId")
                            .inputVariable("jobId", jqpath(".jobbId"))
                    )
            )
            .function(paginate("loop")
                    .variable("fromBatch", "${nextBatch}")
                    .iterate(execute("batch-list")
                            .requiredInput("jobId")
                            .requiredInput("fromBatch")
                    )
                    .prefetchThreshold(100_000)
                    .until(whenVariableIsNull("nextBatch"))
            )
            .function(get("batch-list")
                    .url("${ProduksjonURL}/api/v1/uttrekk/${jobId}/batch/${fromBatch}")
                    .validate(status().success(200))
                    .pipe(sequence(jqpath(".dokumentidentifikator[]"))
                            .expected(jqpath("."))
                    )
                    .pipe(nextPage().output("nextBatch", eval("${cast.toLong(fromBatch) + 1}")))
                    .pipe(parallel(jqpath(".dokumentidentifikator[]"))
                            .variable("position", jqpath("."))
                            .pipe(addContent("${position}", "entry"))
                            .pipe(execute("person-document")
                                    .inputVariable("personDocumentId", jqpath("."))
                            )
                            .pipe(publish("${position}"))
                    )
                    .returnVariables("nextBatch")
            )
            .function(get("person-document")
                    .url("${ProduksjonURL}/api/v1/personer/arkiv/${personDocumentId}?part=person-basis&part=identitetsgrunnlag-utvidet&part=relasjon-utvidet&part=utlendingsmyndighetenesIdentifikasjonsnummer&part=innflytting&part=utflytting&part=foedselINorge&part=opphold&part=forholdTilSametingetsValgmanntall")
                    .header("Accept", "application/xml")
                    .validate(status().success(200))
                    .pipe(addContent("${position}", "person"))
            );

    @Disabled
    @Test
    public void fregUttrekkBatch() {
        Worker.newBuilder()
                .configuration(new StoreBasedDynamicConfiguration.Builder()
                        .values("data.collector.worker.threads", "20")
                        .values("content.stream.connector", "rawdata")
                        .values("rawdata.client.provider", "filesystem")
                        .values("filesystem.storage-folder", "target/avro/rawdata-store")
                        .values("local-temp-folder", "target/avro/temp")
                        .values("avro-file.max.seconds", "60")
                        .values("avro-file.max.bytes", "67108864")
                        .values("avro-file.sync.interval", "20")
                        .values("listing.min-interval-seconds", "0")
                        .values("gcs.bucket-name", "")
                        .values("gcs.listing.min-interval-seconds", "30")
                        .values("gcs.service-account.key-file", "")
                        .environment("DC_")
                        .build()
                        .asMap())
                .buildCertificateFactory(Paths.get("/Volumes/SSB BusinessSSL/certs"))
                .printConfiguration()
                .specification(specificationBuilder)
//                .stopAtNumberOfIterations(100_000)
                .build()
                .run();
    }
}
