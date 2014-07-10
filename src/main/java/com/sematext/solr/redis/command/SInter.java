package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.MultiKeyCommands;
import java.util.Map;

public final class SInter implements Command<MultiKeyCommands> {
  private static final Logger log = LoggerFactory.getLogger(SInter.class);

  @Override
  public Map<String, Float> execute(final MultiKeyCommands client, final SolrParams params) {
    final String[] keys = ParamUtil.getStringByPrefix(params, "key");

    log.debug("Fetching SINTER from Redis for keys: {}", keys);

    return ResultUtil.stringIteratorToMap(client.sinter(keys));
  }
}
