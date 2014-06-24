package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Arrays;
import java.util.Map;

public class GET implements Command {
  private static final Logger log = LoggerFactory.getLogger(GET.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    log.debug("Fetching GET from Redis for key: {}", key);

    return ResultUtil.stringIteratorToMap(Arrays.asList(jedis.get(key)));
  }
}
