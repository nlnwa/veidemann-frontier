package no.nb.nna.veidemann.frontier.worker;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ScriptParameterResolver {
    private static final Logger LOG = LoggerFactory.getLogger(ScriptParameterResolver.class);
    private final Frontier frontier;
    private final LoadingCache<ConfigRef, Map<String, Annotation>> jobParametersCache;

    public ScriptParameterResolver(Frontier frontier) {
        this.frontier = frontier;
        jobParametersCache = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<ConfigRef, Map<String, Annotation>>() {
                            public Map<String, Annotation> load(ConfigRef jobConfigRef) throws DbException {
                                return LoadScriptParametersForJob(jobConfigRef);
                            }
                        });
    }

    public Collection<Annotation> GetScriptParameters(ConfigObject seed, ConfigObject jobConfig) throws DbException {
        Map<String, Annotation> jobAnnotations = GetScriptParametersForJob(jobConfig);
        return GetScriptAnnotationOverridesForSeed(seed, jobConfig, jobAnnotations).values();
    }

    public Map<String, Annotation> GetScriptParametersForJob(ConfigRef jobConfigRef) throws DbException {
        try {
            return jobParametersCache.get(jobConfigRef);
        } catch (ExecutionException e) {
            throw new DbQueryException(e);
        }
    }

    public Map<String, Annotation> GetScriptParametersForJob(ConfigObject jobConfig) throws DbException {
        try {
            return jobParametersCache.get(ApiTools.refForConfig(jobConfig));
        } catch (ExecutionException e) {
            throw new DbQueryException(e);
        }
    }

    private Map<String, Annotation> LoadScriptParametersForJob(ConfigRef jobConfigRef) throws DbException {
        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        ConfigObject jobConfig = frontier.getConfig(jobConfigRef);
        ConfigObject crawlConfig = frontier.getConfig(jobConfig.getCrawlJob().getCrawlConfigRef());
        ConfigObject browserConfig = frontier.getConfig(crawlConfig.getCrawlConfig().getBrowserConfigRef());

        Map<String, Annotation> annotations = new HashMap<>();

        // Get scope script annotations
        frontier.getConfig(jobConfig.getCrawlJob().getScopeScriptRef()).getMeta().getAnnotationList()
                .forEach(a -> annotations.put(a.getKey(), a));

        // Get annotations for referenced browser scripts
        browserConfig.getBrowserConfig().getScriptRefList().forEach(r -> {
            try {
                frontier.getConfig(r).getMeta().getAnnotationList().forEach(a -> annotations.put(a.getKey(), a));
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        });

        // Get annotations for browser scripts matching selectors
        db.listConfigObjects(ListRequest.newBuilder().setKind(Kind.browserScript).addAllLabelSelector(
                browserConfig.getBrowserConfig().getScriptSelectorList()).build()).stream()
                .flatMap(s -> s.getMeta().getAnnotationList().stream())
                .forEach(a -> annotations.put(a.getKey(), a));

        // Override with job specific annotations
        jobConfig.getMeta().getAnnotationList().stream()
                .filter(a -> annotations.containsKey(a.getKey()))
                .forEach(a -> annotations.put(a.getKey(), a));

        return Collections.unmodifiableMap(annotations);
    }

    public Map<String, Annotation> GetScriptAnnotationOverridesForSeed(
            ConfigObject seed, ConfigObject jobConfig, Map<String, Annotation> annotations) throws DbException {

        Map<String, Annotation> result = new HashMap<>();
        result.putAll(annotations);

        if (seed.getSeed().hasEntityRef()) {
            overrideAnnotation(frontier.getConfig(seed.getSeed().getEntityRef()).getMeta().getAnnotationList(), jobConfig, result);
        }

        overrideAnnotation(seed.getMeta().getAnnotationList(), jobConfig, result);

        return result;
    }

    private void overrideAnnotation(List<Annotation> annotations, ConfigObject jobConfig, Map<String, Annotation> jobAnnotations) {
        List<Annotation> ann = new ArrayList<>();
        ann.addAll(annotations);
        for (Iterator<Annotation> it = ann.iterator(); it.hasNext(); ) {
            Annotation a = it.next();
            if (jobAnnotations.containsKey(a.getKey())) {
                jobAnnotations.put(a.getKey(), a);
                it.remove();
            }
        }
        for (Annotation a : ann) {
            if (a.getKey().startsWith("{")) {
                int endIdx = a.getKey().indexOf('}');
                if (endIdx == -1) {
                    throw new IllegalArgumentException("Missing matching '}' for annotation: " + a.getKey());
                }
                String jobIdOrName = a.getKey().substring(1, endIdx);
                String key = a.getKey().substring(endIdx + 1);
                if ((jobConfig.getId().equals(jobIdOrName) || jobConfig.getMeta().getName().equals(jobIdOrName))
                        && jobAnnotations.containsKey(key)) {
                    a = a.toBuilder().setKey(key).build();
                    jobAnnotations.put(a.getKey(), a);
                }
            }
        }
    }
}
