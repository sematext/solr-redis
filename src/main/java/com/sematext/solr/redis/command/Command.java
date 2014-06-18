package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface Command {
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params);

  final static class ResultUtil {
    protected static Map<String, Float> stringIteratorToMap(Iterable<String> collection) {
      return stringIteratorToMap(collection, Float.NaN, new Scorer() {
        @Override
        public Float score(Float v) {
          return v;
        }
      });
    }

    static Map<String, Float> stringIteratorToMap(Iterable<String> collection, Float value, Scorer scorer) {
      final Map<String, Float> map = new HashMap<>();

      for (String entry : collection) {
        value = scorer.score(value);
        map.put(entry, value);
      }

      return map;
    }

    static Map<String, Float> tupleIteratorToMap(Iterable<Tuple> set) {
      final Map<String, Float> map = new HashMap<>();

      for (Tuple tuple : set) {
        map.put(tuple.getElement(), (float) tuple.getScore());
      }

      return map;
    }

    interface Scorer {
      public Float score(Float v);
    }
  }

  final static class ParamUtil {
    static Integer getIntByName(SolrParams params, String param, Integer defaultValue) {
      final String value = params.get(param);
      if (isEmpty(value)) {
        return defaultValue;
      }

      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException ex) {
        return defaultValue;
      }
    }

    static String getStringByName(SolrParams params, String param, String defaultValue) {
      final String value = params.get(param);
      if (isEmpty(value)) {
        return defaultValue;
      }

      return value;
    }

    static String[] getStringByPrefix(SolrParams params, String prefix) {
      final Iterator<String> it = params.getParameterNamesIterator();
      final ArrayList<String> keyList = new ArrayList<>();

      while (it != null && it.hasNext()) {
        final String paramKey = it.next();
        if (paramKey.length() < prefix.length() || !paramKey.substring(0, prefix.length()).equals(prefix)) {
          continue;
        }

        final String newKey = getStringByName(params, paramKey, "");
        if (!newKey.equals("")) {
          keyList.add(newKey);
        }
      }

      return keyList.toArray(new String[keyList.size()]);
    }

    private static boolean isEmpty(String value) {
      return value == null || "".equals(value);
    }
  }
}
