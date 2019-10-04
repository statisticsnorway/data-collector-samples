package no.ssb.dc.samples.ske.sirius;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

public class SiriusWorkerTest {

    @Ignore
    @Test
    public void thatWorkerCollectSiriusFlow() throws InterruptedException {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder().values("content.store.provider", "discarding").build();

        CompletableFuture<ExecutionContext> future = Worker.newBuilder()
                .flow(Flow.start("Collect Sirius", "loop")
                        .configure(context()
                                .topic("sirius")
                                .header("accept", "application/xml")
                                .variable("baseURL", "https://api-at.sits.no")
                                //.variable("fromSequence", "${contentStore.lastPosition(topic) == null ? initialPosition : contentStore.lastPosition(topic)}")
                        )
                        .node(paginate("loop")
                                .variable("fromSequence", "${nextSequence}")
                                .addPageContent()
                                .step(execute("part"))
                                .prefetchThreshold(0.5)
                                .until(whenVariableIsNull("nextSequence"))
                        )
                        .node(get("part")
                                .url("${baseURL}/api/formueinntekt/skattemelding/utkast/hendelser/?fraSekvensnummer=${fromSequence}&antall=100")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .step(sequence(xpath("/hendelser/hendelse"))
                                        .expected(xpath("/hendelse/sekvensnummer"))
                                )
                                .step(nextPage()
                                        .output("nextSequence",
                                                eval(xpath("/hendelser/hendelse[last()]/sekvensnummer"), "lastSequence", "${cast.toLong(lastSequence) + 1}")
                                        )
                                )
                                .step(parallel(xpath("/hendelser/hendelse"))
                                        .variable("position", xpath("/hendelse/sekvensnummer"))
                                        .step(addContent("${position}", "entry"))
                                        .step(execute("utkast-melding")
                                                .inputVariable("utkastIdentifikator", xpath("/hendelse/identifikator"))
                                                .inputVariable("year", xpath("/hendelse/gjelderPeriode"))
                                        )
                                        .step(publish("${position}"))
                                )
                                .returnVariables("nextSequence")
                        )
                        .node(Builders.get("utkast-melding")
                                .url("${baseURL}/api/formueinntekt/skattemelding/utkast/ssb/${year}/${utkastIdentifikator}")
                                .step(addContent("${position}", "utkastIdentifikator"))
                        ))
                .configurationMap(configuration.asMap())
                .sslContext(CommonUtils.currentPath(), "certs")
                .initialPosition("1")
                .initialPositionVariable("fromSequence")
                .maxNumberOfParallelIterations(1)
                .printExecutionPlan()
                .printConfiguration()
                .build()
                .run();

        future.join();
    }
}
