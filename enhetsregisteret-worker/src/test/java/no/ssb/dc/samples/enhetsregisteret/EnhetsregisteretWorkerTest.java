package no.ssb.dc.samples.enhetsregisteret;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.JqPath;
import no.ssb.dc.api.node.builder.JqPathBuilder;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.handler.Queries;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static no.ssb.dc.api.Builders.*;

//https://data.brreg.no/enhetsregisteret/api/docs/index.html
public class EnhetsregisteretWorkerTest {

    static final SpecificationBuilder specificationBuilder = Specification.start("Enhetsregisteret", "Collect enhetsregiseret", "loop-pages-until-done")
            .configure(context()
                    .topic("api")
                    .header("accept", "application/json")
                    .variable("baseURL", "https://data.brreg.no/enhetsregisteret/api")
//                    .variable("nextPage", "${contentStream.hasLastPosition ? cast.toLong(contentStream.lastPosition) : 0}")
                    .variable("nextPage", "${cast.toLong(contentStream.lastOrInitialPosition(-1)) + 1}")
                    .variable("pageSize", "20")
            )
            .function(paginate("loop-pages-until-done")
                    .variable("fromPage", "${nextPage}")
                    .addPageContent("fromPage")
                    .iterate(execute("enheter-page"))
                    .prefetchThreshold(30)
                    .until(whenVariableIsNull("fromPage")))

            .function(get("enheter-page")
                    .url("${baseURL}/enheter/?page=${fromPage}&size=${pageSize}")
                    .validate(status().success(200))
                    .pipe(sequence(jqpath("._embedded.enheter"))
                            .expected(jqpath(".organisasjonsnummer"))
                    )
                    .pipe(nextPage().output("nextPage", jqpath(".page.number")))
            );

    @Test
    void thatEndpointReturnsSomeStuff() {
        // fetch page document
        Request request = Request.newRequestBuilder()
                .GET()
                .url("https://data.brreg.no/enhetsregisteret/api/enheter/?page=1&size=20")
                .build();
        Response response = Client.newClient().send(request);
        JsonParser jsonParser = JsonParser.createJsonParser();
        JsonNode rootNode = jsonParser.fromJson(new String(response.body()), JsonNode.class);
        //System.out.printf("%s%n", jsonParser.toPrettyJSON(rootNode));

        // get page number
        {
            JqPath jqPathPageNumber = new JqPathBuilder(".page.number").build();
            QueryFeature queryPageNumber = Queries.from(jqPathPageNumber);
            String pageNumber = queryPageNumber.evaluateStringLiteral(rootNode);
            System.out.printf("page-number: %s%n", pageNumber);
        }

        // fetch page containing an array of enheter (units)
        // split the array, so we can construct all Jq-queries in order to tell the data collector how to do that
        {
            JqPath jqPathEnhetArray = new JqPathBuilder("._embedded.enheter[]").build();
            QueryFeature queryEnhetEntryArray = Queries.from(jqPathEnhetArray);
            List<?> enhetList = queryEnhetEntryArray.evaluateList(rootNode);
            System.out.printf("entries: %s%n", enhetList.size());
        }
    }

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