package no.ssb.dc.samples.enhetsregisteret;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static no.ssb.dc.api.Builders.*;

public class DownloadEnhetregisterWorkerTest {

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .values("content.stream.connector", "rawdata")
            .values("rawdata.client.provider", "filesystem")
            .values("data.collector.worker.threads", "20")
            .values("rawdata.topic", "enhetsregister")
            .values("local-temp-folder", "target/avro/temp")
            .values("filesystem.storage-folder", "target/avro/rawdata-store")
            .values("avro-file.max.seconds", "60")
            .values("avro-file.max.bytes", Long.toString(64 * 1024 * 1024)) // 512 MiB
            .values("avro-file.sync.interval", Long.toString(200))
            .values("listing.min-interval-seconds", "0")
            .values("data.collector.http.client.timeout.seconds", "3600")
            .values("data.collector.http.request.timeout.seconds", "3600")
            .environment("DC_")
            .build();

    static final SpecificationBuilder specificationBuilder = Specification.start("Enhetsregisteret", "Collect enhetsregiseret", "enheter-download")
            .configure(context()
                    .topic("enhetsregister")
            )
            .function(get("enheter-download")
                    .url("https://data.brreg.no/enhetsregisteret/api/enheter/lastned")
                    .validate(status().success(200))
                    .pipe(sequence(jsonToken())
                            .expected(jqpath(".organisasjonsnummer"))
                    )
                    .pipe(parallel(jsonToken())
                            .variable("position", jqpath(".organisasjonsnummer"))
                            .pipe(addContent("${position}", "enhet"))
                            .pipe(publish("${position}"))
                    )
            );

    @Disabled
    @Test
    void downloadEnhetregister() {
        Worker.newBuilder()
                .specification(specificationBuilder)
                .configuration(configuration.asMap())
                .build()
                .run();
    }
}
