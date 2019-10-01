package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.Flow;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
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

    public static Flow getFlow(String baseURL) {
        return Flow.start("Collect Sirius", "loop")
                .node(paginate("loop")
                        .variable("fromSeq", "${nextSeq}")
                        .addPageContent()
                        .step(execute("part"))
                        .prefetchThreshold(0.5)
                        .until(whenVariableIsNull("nextSeq"))
                )
                .node(get("part")
                        .header("accept", "application/xml")
                        .url(baseURL + "/api/formueinntekt/skattemelding/utkast/hendelser/?fraSekvensnummer=${fromSeq}&antall=100")
                        .step(sequence(xpath("/hendelser/hendelse"))
                                .expected(xpath("/hendelse/sekvensnummer"))
                        )
                        .step(nextPage().output("nextSeq", eval(xpath("/hendelser/hendelse[last()]/sekvensnummer"), "lastSeq", "${cast.toLong(lastSeq) + 1}"))
                        )
                        .step(parallel(xpath("/hendelser/hendelse"))
                                .variable("position", xpath("/hendelse/sekvensnummer"))
                                .step(addContent("${position}", "entry"))
                                .step(execute("utkast-melding")
                                        .inputVariable("utkastId", xpath("/hendelse/identifikator"))
                                )
                                .step(publish("${position}"))
                        )
                        .returnVariables("nextSeq")
                )
                .end();
    }
}
