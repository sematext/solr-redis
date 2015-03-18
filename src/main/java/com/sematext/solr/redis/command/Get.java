package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedisCommands;
import java.util.Map;

public final class Get implements Command<BinaryJedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(Get.class);
  private final ValueFilter valueFilter;

  public Get(final ValueFilter valueFilter) {
    this.valueFilter = valueFilter;
  }

  @Override
  public Map<String, Float> execute(final BinaryJedisCommands client, final SolrParams params) {
    final String key = ParamUtil.assertGetStringByName(params, "key");
    final byte[] byteValue = client.get(key.getBytes());

    if (byteValue == null) {
      return null;
    }

    log.debug("Fetching GET from Redis for key: {}", key);

    try {
      return valueFilter.filterValue(params, byteValue);
    } catch (final UnsupportedAlgorithmException e) {
      log.error(e.getMessage());
      return null;
    } catch (final DeserializationException e) {
      log.error(e.getMessage());
      return null;
    }
  }
}
