package no.ssb.dc.samples.enhetsregisteret;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
                    .prefetchThreshold(1500)
                    .until(whenVariableIsNull("nextSequence")))

            .function(get("enheter")
                    .url("${URL}/enheter/?page${fromSequence}&size=20")
                    .validate(status().success(200).fail(400).fail(404).fail(500))

            );


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