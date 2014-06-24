package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class KEYS implements Command {
  private static final Logger log = LoggerFactory.getLogger(KEYS.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {

    log.debug("Fetching KEYS from Redis for key: {}", key);

    return ResultUtil.stringIteratorToMap(jedis.keys(key));
  }
}
