package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import org.assertj.core.api.AbstractAssert;

public class ErrorAssert extends AbstractAssert<ErrorAssert, Error> {
    public ErrorAssert(Error actual) {
        super(actual, ErrorAssert.class);
    }

    public ErrorAssert codeEquals(int expected) {
        isNotNull();
        if (actual.getCode() != expected) {
            failWithMessage("Expected error code to be <%d>, but was <%d>",
                    expected, actual.getCode());
        }
        return this;
    }

    public ErrorAssert codeEquals(ExtraStatusCodes expected) {
        isNotNull();
        if (actual.getCode() != expected.getCode()) {
            failWithMessage("Expected error code to be <%d>, but was <%d>",
                    expected.getCode(), actual.getCode());
        }
        return this;
    }

    public ErrorAssert msgEquals(String expected) {
        isNotNull();
        if (!actual.getMsg().equals(expected)) {
            failWithMessage("Expected error message to be <%s>, but was <%s>",
                    expected, actual.getMsg());
        }
        return this;
    }

    public ErrorAssert detailEquals(String expected) {
        isNotNull();
        if (!actual.getDetail().equals(expected)) {
            failWithMessage("Expected error detail to be <%s>, but was <%s>",
                    expected, actual.getDetail());
        }
        return this;
    }
}
