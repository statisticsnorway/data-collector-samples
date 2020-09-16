package no.ssb.dc.samples.enhetsregisteret;

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

//https://data.brreg.no/enhetsregisteret/api/docs/index.html
public class EnhetsregisteretWorkerTest {

    static final SpecificationBuilder specificationBuilder = Specification.start("Enhetsregisteret", "Collect enhetsregiseret", "loop")
            .configure(context()
                    .topic("api")
                    .header("accept", "application/json")
                    .variable("URL", "https://data.brreg.no/enhetsregisteret/api")
                    .variable("nextSequence", "${cast.toLong(contentStream.lastOrInitialPosition(0)) + 1}")
            )
            .function(paginate("loop")
                    .variable("fromSequence", "${nextSequence}")
                    .addPageContent("fromSequence")
                    .iterate(execute("enheter"))
                    .prefetchThreshold(1500)
                    .until(whenVariableIsNull("nextSequence")))

            .function(get("enheter")
                    .url("${URL}/enheter/?page=${fromSequence}&size=20")
                    .validate(status().success(200).fail(400).fail(404).fail(500))

            );



    @Test
    public void thatWorkerCollectEnhetsregisteret() {
        Worker.newBuilder()
                .configuration(new StoreBasedDynamicConfiguration.Builder()
                        .values("content.stream.connector", "rawdata")
                        .values("rawdata.client.provider", "memory")
                        .values("data.collector.worker.threads", "20")
                        .environment("DC_")
                        .build()
                        .asMap())
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

        Files.writeString(targetPath.resolve("specs").resolve("enhetsregisteret-test-spec.json"), specificationBuilder.serialize());
    }
}