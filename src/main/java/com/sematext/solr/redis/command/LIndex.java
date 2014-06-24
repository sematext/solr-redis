package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import java.util.Arrays;
import java.util.Map;

public class LIndex implements Command {
  private static final Logger log = LoggerFactory.getLogger(LIndex.class);

  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final Integer index = ParamUtil.getIntByName(params, "index", 0);

    log.debug("Fetching LINDEX from Redis for key: {} ({})", key, index);

    return ResultUtil.stringIteratorToMap(Arrays.asList(jedis.lindex(key, index)));
  }
}
