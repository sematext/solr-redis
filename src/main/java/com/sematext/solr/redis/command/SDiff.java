package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class SDiff implements Command {
  private static final Logger log = LoggerFactory.getLogger(SDiff.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String[] keys = ParamUtil.getStringByPrefix(params, "key");

    log.debug("Fetching SDIFF from Redis for keys: {}", keys);

    return ResultUtil.stringIteratorToMap(jedis.sdiff(keys));
  }
}
