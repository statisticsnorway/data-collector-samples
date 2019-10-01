package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.Flow;
import no.ssb.dc.api.node.builder.FlowBuilder;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.validateRequest;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

public class SiriusFlow {

    private SiriusFlow() {
    }

    public static FlowBuilder getFlow() {
        return Flow.start("Collect Sirius", "loop")
                .node(paginate("loop")
                        .variable("fromSequence", "${nextSequence}")
                        .addPageContent()
                        .step(execute("part"))
                        .prefetchThreshold(0.5)
                        .until(whenVariableIsNull("nextSequence"))
                )
                .node(get("part")
                        .header("accept", "application/xml")
                        .url("${baseURL}/api/formueinntekt/skattemelding/utkast/hendelser/?fraSekvensnummer=${fromSequence}&antall=100")
                        .step(validateRequest())
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
                .node(get("utkast-melding")
                        .url("${baseURL}/api/formueinntekt/skattemelding/utkast/ssb/${year}/${utkastIdentifikator}")
                        .step(addContent("${position}", "utkastIdentifikator"))
                );
    }
}
