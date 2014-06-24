package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class ZRevrangeByScore implements Command {
  private static final Logger log = LoggerFactory.getLogger(ZRevrangeByScore.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String min = ParamUtil.getStringByName(params, "min", "-inf");
    final String max = ParamUtil.getStringByName(params, "max", "+inf");

    log.debug("Fetching ZREVRANGEBYSCORE from Redis for key: {} ({}, {})", key, min, max);

    return Command.ResultUtil.tupleIteratorToMap(jedis.zrevrangeByScoreWithScores(key, max, min));
  }
}
