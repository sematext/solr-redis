package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import java.util.Arrays;
import java.util.Map;

public final class LIndex implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(LIndex.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");
    final int index = ParamUtil.tryGetIntByName(params, "index", 0);

    log.debug("Fetching LINDEX from Redis for key: {} ({})", key, index);

    return ResultUtil.stringIteratorToMap(Arrays.asList(client.lindex(key, index)));
  }
}
