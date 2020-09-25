package no.nb.nna.veidemann.frontier.db.script;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class LuaScript {
    String scriptName;
    String sha;
    String script;

    public LuaScript(String scriptName) {
        this.scriptName = scriptName;
        InputStream in = LuaScript.class.getClassLoader().getResourceAsStream("lua/" + scriptName);
        script = new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }

    Object runString(Jedis jedis, List<String> keys, List<String> args) {
        if (sha == null) {
            sha = jedis.scriptLoad(script);
        }
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException ex) {
            sha = null;
            return runString(jedis, keys, args);
        }
    }

    Object runBytes(Jedis jedis, List<byte[]> keys, List<byte[]> args) {
        if (sha == null) {
            sha = jedis.scriptLoad(script);
        }
        try {
            return jedis.evalsha(sha.getBytes(), keys, args);
        } catch (JedisNoScriptException ex) {
            sha = null;
            return runBytes(jedis, keys, args);
        }
    }
}
