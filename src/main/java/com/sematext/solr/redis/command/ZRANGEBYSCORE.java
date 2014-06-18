package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class ZRANGEBYSCORE implements Command {
  private static final Logger log = LoggerFactory.getLogger(ZRANGEBYSCORE.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String min = ParamUtil.getStringByName(params, "min", "-inf");
    final String max = ParamUtil.getStringByName(params, "max", "+inf");

    log.debug("Fetching ZRANGEBYSCORE from Redis for key: {} ({}, {})", key, min, max);

    return Command.ResultUtil.tupleIteratorToMap(jedis.zrangeByScoreWithScores(key, max, min));
  }
}
