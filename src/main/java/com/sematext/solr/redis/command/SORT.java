package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import java.util.Map;

public class SORT implements Command {
  @Override
  public Map<String, Float> execute(Jedis jedis, String key, SolrParams params) {
    final String algorithm = ParamUtil.getStringByName(params, "algorithm", null);
    final String order = ParamUtil.getStringByName(params, "order", null);
    final Integer limit = ParamUtil.getIntByName(params, "limit", null);
    final Integer offset = ParamUtil.getIntByName(params, "offset", null);
    final String byValue = ParamUtil.getStringByName(params, "by", null);
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

    sortingParams.get(ParamUtil.getStringByPrefix(params, "get"));

    // Use decrementing Scorer to preserve list ordering
    return ResultUtil.stringIteratorToMap(jedis.sort(key, sortingParams), 0F, new ResultUtil.Scorer() {
      @Override
      public Float score(Float score) {
        return score - 1F;
      }
    });
  }
}
