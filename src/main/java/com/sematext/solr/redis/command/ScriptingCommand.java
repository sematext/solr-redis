package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import redis.clients.jedis.ScriptingCommands;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

abstract class ScriptingCommand implements Command<ScriptingCommands> {

  protected abstract Map<String, Float> invokeCommand(ScriptingCommands client, SolrParams params, int keyLength,
    String[] args);

  @Override
  public final Map<String, Float> execute(final ScriptingCommands client, final SolrParams params) {
    final String[] keys = ParamUtil.getStringByPrefix(params, "key");
    final String[] args = ParamUtil.getStringByPrefix(params, "arg");
    final int keyLength = keys.length;
    final int argLength = args.length;

    final String[] combined = new String[keyLength + argLength];

    System.arraycopy(keys, 0, combined, 0, keyLength);
    System.arraycopy(args, 0, combined, keyLength, argLength);

    return invokeCommand(client, params, keyLength, combined);
  }

  protected final Map<String, Float> createReturnValue(final SolrParams params, final Object result) {
    final boolean returnsHash = ParamUtil.tryGetBooleanByName(params, "returns_hash", false);

    if (result == null) {
      return null;
    }

    if (result instanceof Iterable) {
      return returnsHash ? returnHash((Iterable) result) : returnList((Iterable<String>) result);
    } else {
      return returnScalar(result);
    }
  }

  private Map<String, Float> returnScalar(final Object result) {
    return ResultUtil.stringIteratorToMap(Collections.singletonList(result.toString()));
  }

  private Map<String, Float> returnList(final Iterable<String> result) {
    return ResultUtil.stringIteratorToMap(result);
  }

  private Map<String, Float> returnHash(final Iterable result) {
    final Map<String, Float> hash = new HashMap<>();

    int a = 0;
    String hashKey = null;
    for (final Object value: result) {
      a++;
      if (a % 2 == 0) {
        hash.put(hashKey, Float.parseFloat(value.toString()));
      } else {
        hashKey = value.toString();
      }
    }

    if (a % 2 != 0) {
      throw new IllegalArgumentException(String.format("Expected list with even number of elements, got %d", a));
    }

    return hash;
  }
}
