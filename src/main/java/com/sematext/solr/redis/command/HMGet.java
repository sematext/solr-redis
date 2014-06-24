package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class HMGet implements Command {
  private static final Logger log = LoggerFactory.getLogger(HMGet.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String[] fields = ParamUtil.getStringByPrefix(params, "field");

    log.debug("Fetching HMGET from Redis for key: {} ({})", key, fields);

    return ResultUtil.stringIteratorToMap(jedis.hmget(key, fields));
  }
}
