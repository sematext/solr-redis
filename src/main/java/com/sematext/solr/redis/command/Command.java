package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import java.util.Map;

public interface Command<T> {
  Map<String, Float> execute(T client, String key, SolrParams params);
}
