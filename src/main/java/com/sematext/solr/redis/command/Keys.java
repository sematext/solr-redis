package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.MultiKeyCommands;
import java.util.Map;

public final class Keys implements Command<MultiKeyCommands> {
  private static final Logger log = LoggerFactory.getLogger(Keys.class);

  @Override
  public Map<String, Float> execute(final MultiKeyCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");

    log.debug("Fetching KEYS from Redis for key: {}", key);

    return ResultUtil.stringIteratorToMap(client.keys(key));
  }
}
