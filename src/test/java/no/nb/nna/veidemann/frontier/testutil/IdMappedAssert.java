package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.frontier.testutil.IdMappedAssert.FromMapAssert;
import org.assertj.core.api.AbstractAssert;

import java.util.Map;

public class IdMappedAssert<ASSERT extends FromMapAssert<ASSERT, MAPPED>, MAPPED>
        extends AbstractAssert<IdMappedAssert<ASSERT, MAPPED>, Map<String, MAPPED>> {

    protected final Class<MAPPED> mappedType;
    protected final Class<ASSERT> assertType;

    public IdMappedAssert(Map<String, MAPPED> actual, Class<ASSERT> assertType, Class<MAPPED> mappedType) {
        super(actual, IdMappedAssert.class);
        this.mappedType = mappedType;
        this.assertType = assertType;
    }

    public IdMappedAssert<ASSERT, MAPPED> hasNumberOfElements(int expected) {
        if (actual.size() != expected) {
            failWithMessage("Expected number of CrawlHostGroups to be <%d>, but was <%d>",
                    expected, actual.size());
        }
        return this;
    }

    public ASSERT elementById(String id) {
        try {
            return (ASSERT) assertType.getDeclaredConstructor(IdMappedAssert.class, mappedType).newInstance(this, actual.get(id));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class FromMapAssert<SELF extends FromMapAssert<SELF, MAPPED>, MAPPED> extends AbstractAssert<SELF, MAPPED> {
        final IdMappedAssert origin;

        public FromMapAssert(IdMappedAssert origin, MAPPED actual) {
            super(actual, FromMapAssert.class);
            this.origin = origin;
        }

        public SELF elementById(String id) {
            return (SELF) origin.elementById(id);
        }
    }
}
