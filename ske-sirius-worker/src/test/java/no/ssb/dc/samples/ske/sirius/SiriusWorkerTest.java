package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.Flow;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.testng.annotations.Test;

import java.util.Map;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.security;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

// https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_skattemelding
public class SiriusWorkerTest {

//    @Ignore
    @Test
    public void thatWorkerCollectSiriusData() throws InterruptedException {
        Worker.newBuilder()
                .configuration(Map.of(
                        "content.store.provider", "rawdata",
                        "rawdata.client.provider", "memory",
                        "data.collector.worker.threads", "50")
                )
                .buildCertificateFactory(CommonUtils.currentPath())
                //.stopAtNumberOfIterations(5)
                .printConfiguration()
                .printExecutionPlan()
                .flow(Flow.start("Collect Sirius", "loop")
                        .configure(context()
                                .topic("sirius")
                                .header("accept", "application/xml")
                                .variable("baseURL", "https://api-at.sits.no")
                                .variable("rettighetspakke", "ssb")
                                .variable("hentAntallMeldingerOmGangen", "100")
                                .variable("fromSequence", "1")
                                .variable("hendelse", "utkast")
                                //.variable("hendelse", "fastsatt")
                                //.variable("fromSequence", "${contentStore.lastPosition(topic) == null ? initialPosition : contentStore.lastPosition(topic)}")
                        )
                        .configure(security()
                                .sslBundleName("ske-test-certs")
                        )
                        .function(paginate("loop")
                                .variable("fromSequence", "${nextSequence}")
                                .addPageContent()
                                .iterate(execute("parts"))
                                .prefetchThreshold(25)
                                .until(whenVariableIsNull("nextSequence"))
                        )
                        .function(get("parts")
                                .url("${baseURL}/api/formueinntekt/skattemelding/utkast/hendelser/?fraSekvensnummer=${fromSequence}&antall=${hentAntallMeldingerOmGangen}")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .pipe(sequence(xpath("/hendelser/hendelse"))
                                        .expected(xpath("/hendelse/sekvensnummer"))
                                )
                                .pipe(nextPage()
                                        .output("nextSequence",
                                                eval(xpath("/hendelser/hendelse[last()]/sekvensnummer"), "lastSequence", "${cast.toLong(lastSequence) + 1}")
                                        )
                                )
                                .pipe(parallel(xpath("/hendelser/hendelse"))
                                        .variable("position", xpath("/hendelse/sekvensnummer"))
                                        .pipe(addContent("${position}", "entry"))
                                        .pipe(execute("utkast-melding")
                                                .inputVariable("utkastIdentifikator", xpath("/hendelse/identifikator"))
                                                .inputVariable("year", xpath("/hendelse/gjelderPeriode"))
                                        )
                                        .pipe(publish("${position}"))
                                )
                                .returnVariables("nextSequence")
                        )
                        .function(get("utkast-melding")
                                .url("${baseURL}/api/formueinntekt/skattemelding/${hendelse}/${rettighetspakke}/${year}/${utkastIdentifikator}")
                                .validate(status().success(200).fail(400).fail(404).fail(500))
                                .pipe(addContent("${position}", "skattemelding"))
                        ))
                .build()
                .run();
    }
}
