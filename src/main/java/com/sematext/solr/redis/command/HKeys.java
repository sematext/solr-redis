package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import java.util.Map;

public final class HKeys implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(HKeys.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");

    log.debug("Fetching HKEYS from Redis for key: {}", key);

    return ResultUtil.stringIteratorToMap(client.hkeys(key));
  }
}
