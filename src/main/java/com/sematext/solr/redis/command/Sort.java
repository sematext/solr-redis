package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.SortingParams;
import java.util.Map;

public final class Sort implements Command<JedisCommands> {
  private static final Logger log = LoggerFactory.getLogger(Sort.class);

  @Override
  public Map<String, Float> execute(final JedisCommands client, final String key, final SolrParams params) {
    final String algorithm = ParamUtil.getStringByName(params, "algorithm", null);
    final String order = ParamUtil.getStringByName(params, "order", null);
    final Integer limit = ParamUtil.getIntByName(params, "limit", null);
    final Integer offset = ParamUtil.getIntByName(params, "offset", null);
    final String byValue = ParamUtil.getStringByName(params, "by", null);
    final String[] get = ParamUtil.getStringByPrefix(params, "get");
    final SortingParams sortingParams = new SortingParams();

    if ("alpha".equalsIgnoreCase(algorithm)) {
      sortingParams.alpha();
    }

    if ("desc".equalsIgnoreCase(order)) {
      sortingParams.desc();
    } else if ("asc".equalsIgnoreCase(order)) {
      sortingParams.asc();
    }

    if (limit != null || offset != null) {
      sortingParams.limit(offset == null ? 0 : offset, limit == null ? 0 : limit);
    }

    if (byValue != null) {
      sortingParams.by(byValue);
    }

    sortingParams.get(get);

    log.debug("Fetching SORT from Redis for keys: {} GET {} BY {} ORDER {} OFFSET {} LIMIT {} ALGORITHM {}", key, get,
        byValue, order, offset, limit, algorithm);

    // Use decrementing Scorer to preserve list ordering
    return ResultUtil.stringIteratorToMap(client.sort(key, sortingParams), 0F, new ResultUtil.Scorer() {
      @Override
      public Float score(final Float score) {
        return score - 1F;
      }
    });
  }
}
