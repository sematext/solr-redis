package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import java.util.Map;

public final class SRandMember implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(SRandMember.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");
    final int count = ParamUtil.tryGetIntByName(params, "count", 1);

    log.debug("Fetching SRANDMEMBER from Redis for key: {} ({})", key, count);

    // Workaround for https://github.com/xetorthio/jedis/issues/665
    return client instanceof Jedis ? ResultUtil.stringIteratorToMap(((Jedis) client).srandmember(key, count)) : null;
  }
}
