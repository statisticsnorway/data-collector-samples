package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.node.builder.FlowBuilder;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

public class SiriusFlow {

    private SiriusFlow() {
    }

    public static FlowBuilder get() {
        return Flow.start("Collect Sirius", "loop")
                .node(paginate("loop")
                        .variable("baseURL", "https://api-at.sits.no")
                        .variable("fromSequence", "${nextSequence}")
                        .addPageContent()
                        .step(execute("part"))
                        .prefetchThreshold(0.5)
                        .until(whenVariableIsNull("nextSequence"))
                )
                .node(Builders.get("part")
                        .header("accept", "application/xml")
                        .url("${baseURL}/api/formueinntekt/skattemelding/utkast/hendelser/?fraSekvensnummer=${fromSequence}&antall=100")
                        .validateResponse().success(200)
                        .validateResponse().fail(400)
                        .validateResponse().fail(404)
                        .validateResponse().fail(500)
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
                        .header("accept", "application/xml")
                        .url("${baseURL}/api/formueinntekt/skattemelding/utkast/ssb/${year}/${utkastIdentifikator}")
                        .step(addContent("${position}", "utkastIdentifikator"))
                );
    }
}
