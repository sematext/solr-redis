package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import java.util.Map;

public class ZRange implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(ZRange.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final String key, final SolrParams params) {
    final long start = ParamUtil.getIntByName(params, "range_start", 0);
    final long end = ParamUtil.getIntByName(params, "range_end", -1);

    log.debug("Fetching ZRANGE from Redis for key: {} ({}, {})", key, start, end);

    return ResultUtil.tupleIteratorToMap(client.zrangeWithScores(key, start, end));
  }
}
