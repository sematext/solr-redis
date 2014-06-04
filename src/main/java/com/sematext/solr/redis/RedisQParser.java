package com.sematext.solr.redis;

import java.util.Collection;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

/**
 *
 */
public class RedisQParser extends QParser {
  static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);
  
  protected Collection<String> terms;

  private final JedisCommands redis;

  RedisQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
          JedisCommands redis) {
    super(qstr, localParams, params, req);
    this.redis = redis;

    String redisMethod = localParams.get("method");
    String redisKey = localParams.get("key");

    if (redisMethod.compareToIgnoreCase("smembers") == 0) {
      terms = redis.smembers(redisKey);
    }

    Collection<String> terms = null;
  }

  @Override
  public Query parse() throws SyntaxError {
    String fieldName = localParams.get(QueryParsing.V, null);
    BooleanQuery booleanQuery = new BooleanQuery(true);
    log.debug();
    for (String term : terms) {
      TermQuery termQuery = new TermQuery(new Term(qstr, fieldName));
      booleanQuery.add(termQuery, BooleanClause.Occur.SHOULD);
    }
    return booleanQuery;
  }

}
