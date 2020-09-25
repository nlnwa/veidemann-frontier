package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.config.v1.Annotation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ScopeCheckTest {

    @Test
    public void isExcluded() {
        ScopeCheck sc = new ScopeCheck(null);
        List<Annotation> annotations = new ArrayList<>();

        assertThat(sc.isExcluded("surt", annotations)).isFalse();

        annotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
        assertThat(sc.isExcluded("surt", annotations)).isFalse();

        annotations.add(Annotation.newBuilder().setKey("v7n_scope-exclude").setValue("surts").build());
        assertThat(sc.isExcluded("surt", annotations)).isFalse();

        annotations.add(Annotation.newBuilder().setKey("v7n_scope-exclude").setValue("su").build());
        assertThat(sc.isExcluded("surt", annotations)).isTrue();

        annotations.add(Annotation.newBuilder().setKey("v7n_scope-exclude").setValue("aa bb  cc ").build());
        assertThat(sc.isExcluded("aa", annotations)).isTrue();
        assertThat(sc.isExcluded("bb", annotations)).isTrue();
        assertThat(sc.isExcluded("cc", annotations)).isTrue();
        assertThat(sc.isExcluded("ee", annotations)).isFalse();
        assertThat(sc.isExcluded("aax", annotations)).isTrue();
        assertThat(sc.isExcluded("bbx", annotations)).isTrue();
        assertThat(sc.isExcluded("ccx", annotations)).isTrue();
        assertThat(sc.isExcluded("eex", annotations)).isFalse();
        assertThat(sc.isExcluded("a", annotations)).isFalse();
        assertThat(sc.isExcluded("b", annotations)).isFalse();
        assertThat(sc.isExcluded("c", annotations)).isFalse();
        assertThat(sc.isExcluded("e", annotations)).isFalse();
    }
}
