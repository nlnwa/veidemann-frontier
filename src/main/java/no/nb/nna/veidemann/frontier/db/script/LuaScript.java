package no.nb.nna.veidemann.frontier.db.script;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class LuaScript {
    private static final Logger LOG = LoggerFactory.getLogger(LuaScript.class);

    private final Marker scriptNameMarker;
    String scriptName;
    String sha;
    String script;

    public LuaScript(String scriptName) {
        scriptNameMarker = MarkerFactory.getMarker(scriptName);
        this.scriptName = scriptName;
        InputStream in = LuaScript.class.getClassLoader().getResourceAsStream("lua/" + scriptName);
        script = new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }

    Object runString(Jedis jedis, List<String> keys, List<String> args) {
        if (sha == null) {
            sha = jedis.scriptLoad(script);
        }
        try {
            Object result = jedis.evalsha(sha, keys, args);
            LOG.trace(scriptNameMarker, "{}: KEYS: {}, ARGS: {}, RESULT: {}", scriptName, keys, args, result);
            return result;
        } catch (JedisNoScriptException ex) {
            sha = null;
            return runString(jedis, keys, args);
        }
    }

    Object runBytes(Jedis jedis, List<byte[]> keys, List<byte[]> args) {
        LOG.trace(scriptNameMarker, "{}: KEYS: {}, ARGS: {}", scriptName, keys, args);
        if (sha == null) {
            sha = jedis.scriptLoad(script);
        }
        try {
            Object result = jedis.evalsha(sha.getBytes(), keys, args);
            LOG.trace(scriptNameMarker, "{}: KEYS: {}, ARGS: {}, RESULT: {}", scriptName, keys, args, result);
            return result;
        } catch (JedisNoScriptException ex) {
            sha = null;
            return runBytes(jedis, keys, args);
        }
    }
}
