package com.sematext.solr.redis;

import java.util.Collection;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import redis.clients.jedis.Jedis;

/**
 *
 */
public class RedisQParser extends QParser {
  protected Collection<String> terms;

  private final Jedis jedisConnector;

  RedisQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, Jedis jedisConnector) {
    super(qstr, localParams, params, req);

    String redisMethod = localParams.get("method");
    String redisKey = localParams.get("key");
    this.jedisConnector = jedisConnector;
    if (redisMethod.compareToIgnoreCase("smembers") == 0) {
      
    }

    Collection<String> terms = null;
  }

  @Override
  public Query parse() throws SyntaxError {
    String fieldName = localParams.get(QueryParsing.V, null);
    BooleanQuery booleanQuery = new BooleanQuery(true);
    for (String term : terms) {
      TermQuery termQuery = new TermQuery(new Term(qstr, fieldName));
      booleanQuery.add(termQuery, BooleanClause.Occur.SHOULD);
    }
    return booleanQuery;
  }

}
