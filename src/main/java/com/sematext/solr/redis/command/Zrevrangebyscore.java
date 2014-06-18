package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class Zrevrangebyscore implements Command {

  private static final Logger log = LoggerFactory.getLogger(Zrevrangebyscore.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    String min = params.get("min");
    String max = params.get("max");
    if (min == null || "".equals(min)) {
      min = "-inf";
    }
    if (max == null || "".equals(max)) {
      max = "+inf";
    }

    log.debug("Fetching zrevrangebyscore from Redis for key: {} ({}, {})", key, min, max);

    return Command.ResultUtil.tupleSetToMap(jedis.zrevrangeByScoreWithScores(key, max, min));
  }
}
