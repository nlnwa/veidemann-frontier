package no.nb.nna.veidemann.frontier.testutil;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

import java.util.function.Consumer;

/**
 * Filter for container logs from testcontainers.org
 */
public class SkipUntilFilter extends BaseConsumer<SkipUntilFilter> {
    final String match;
    final Consumer<OutputFrame> next;
    boolean foundMatch = false;

    public SkipUntilFilter(String match, Consumer next) {
        this.match = match;
        this.next = next;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        if (foundMatch) {
//            int hash = outputFrame.getUtf8String().indexOf('#');
//            if (hash > 0) {
//                outputFrame = new OutputFrame(outputFrame.getType(), ("<redis> " + outputFrame.getUtf8String().substring(hash + 1).trim()).getBytes());
//            }
            next.accept(outputFrame);
            return;
        }
        if (outputFrame.getUtf8String().contains(match)) {
            foundMatch = true;
        }
    }
}
