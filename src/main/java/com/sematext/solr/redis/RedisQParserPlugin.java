package com.sematext.solr.redis;

import java.util.Arrays;
import java.util.List;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import redis.clients.jedis.Jedis;

/**
 * Create a boolean query with multiple tokens.
 */
public class RedisQParserPlugin extends QParserPlugin {
  public static final String NAME = "redis";
  public static final String HOST_FIELD = "hosts";

  public final List<String> hosts = Arrays.asList("localhost");

  public Jedis jedisConnector;


  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    String redisMethod = localParams.get("method");
    String redisKey = localParams.get("key");
    return new RedisQParser(qstr, localParams, params, req, jedisConnector);
  }

  @Override
  public void init(NamedList args) {
    if (args != null) {
      Object hosts = args.get(HOST_FIELD);
      if (hosts instanceof NamedList) {
        SolrParams hostsConfiguration = SolrParams.toSolrParams((NamedList) hosts);
      }
    }
  }


}
