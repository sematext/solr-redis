package com.sematext.solr.redis.command;

import redis.clients.jedis.Tuple;
import java.util.HashMap;
import java.util.Map;

final class ResultUtil {
  private ResultUtil() {
    super();
  }

  static Map<String, Float> stringIteratorToMap(final Iterable<String> collection) {
    return stringIteratorToMap(collection, Float.NaN, new Scorer() {
      @Override
      public Float score(final Float v) {
        return v;
      }
    });
  }

  static Map<String, Float> stringIteratorToMap(final Iterable<String> collection, final Float value,
    final Scorer scorer) {
    final Map<String, Float> map = new HashMap<>();
    Float score = value;

    for (final String entry : collection) {
      score = scorer.score(score);
      map.put(entry, score);
    }

    return map;
  }

  static Map<String, Float> tupleIteratorToMap(final Iterable<Tuple> set) {
    final Map<String, Float> map = new HashMap<>();

    for (final Tuple tuple : set) {
      map.put(tuple.getElement(), (float) tuple.getScore());
    }

    return map;
  }

  interface Scorer {
    Float score(final Float v);
  }
}
