package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import java.util.Map;

public final class SMembers implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(SMembers.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final String key, final SolrParams params) {
    log.debug("Fetching SMEMBERS from Redis for key: {}", key);

    return ResultUtil.stringIteratorToMap(client.smembers(key));
  }
}
