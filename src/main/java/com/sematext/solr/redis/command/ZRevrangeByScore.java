package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import java.util.Map;

public final class ZRevrangeByScore implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(ZRevrangeByScore.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");
    final String min = ParamUtil.tryGetStringByName(params, "min", "-inf");
    final String max = ParamUtil.tryGetStringByName(params, "max", "+inf");
    final boolean withScores = ParamUtil.tryGetBooleanByName(params, "with_scores", true);

    log.debug("Fetching ZREVRANGEBYSCORE from Redis for key: {} ({}, {})", key, min, max);

    if (withScores) {
      return ResultUtil.tupleIteratorToMap(client.zrevrangeByScoreWithScores(key, min, max));
    } else {
      return ResultUtil.stringIteratorToMap(client.zrevrangeByScore(key, min, max));
    }
  }
}
