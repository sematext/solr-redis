package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class LRange implements Command {
  private static final Logger log = LoggerFactory.getLogger(LRange.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final Integer min = ParamUtil.getIntByName(params, "min", 0);
    final Integer max = ParamUtil.getIntByName(params, "max", -1);

    log.debug("Fetching LRANGE from Redis for key: {} ({}, {})", key, min, max);

    // Use decrementing Scorer to preserve list ordering
    return ResultUtil.stringIteratorToMap(jedis.lrange(key, min, max), 0F, new ResultUtil.Scorer() {
      @Override
      public Float score(Float score) {
        return score - 1F;
      }
    });
  }
}
