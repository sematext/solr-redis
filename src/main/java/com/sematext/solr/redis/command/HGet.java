package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Map;

public class HGet implements Command {
  private static final Logger log = LoggerFactory.getLogger(HGet.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String field = ParamUtil.getStringByName(params, "field", null);

    if (field == null) {
      throw new IllegalArgumentException("Required parameter \"field\" missing");
    }

    log.debug("Fetching HGET from Redis for key: {} ({})", key, field);

    return ResultUtil.stringIteratorToMap(Arrays.asList(jedis.hget(key, field)));
  }
}
