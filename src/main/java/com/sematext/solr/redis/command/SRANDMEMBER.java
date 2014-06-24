package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class SRANDMEMBER implements Command {
  private static final Logger log = LoggerFactory.getLogger(SRANDMEMBER.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final Integer count = ParamUtil.getIntByName(params, "count", 1);

    log.debug("Fetching SRANDMEMBER from Redis for key: {} ({})", key, count);

    return ResultUtil.stringIteratorToMap(jedis.srandmember(key, count));
  }
}
