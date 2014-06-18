package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface Command {
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params);

  class ResultUtil {
    public static Map<String, Float> iteratorToMap(Iterable<String> collection) {
      return iteratorToMap(collection, Float.NaN);
    }

    public static Map<String, Float> iteratorToMap(Iterable<String> collection, Float value) {
      final Map<String, Float> map = new HashMap<>();

      for (String entry : collection) {
        map.put(entry, value);
      }

      return map;
    }

    public static Map<String, Float> tupleSetToMap(Set<Tuple> set) {
      final Map<String, Float> map = new HashMap<>();

      for (Tuple tuple : set) {
        map.put(tuple.getElement(), (float) tuple.getScore());
      }

      return map;
    }
  }
}
