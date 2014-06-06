package com.sematext.solr.redis;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

/**
 * RedisQParser is responsible for preparing a query based on data fetched from Redis.
 */
public class RedisQParser extends QParser {
  private static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  private static final Set<String>  ALLOWED_METHODS = new HashSet<String>(){
      {
        add("smembers");
      }
  };

  private final JedisCommands jedis;
  private Collection<String> redisObjectsCollection = null;
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;

  RedisQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, JedisCommands jedis) {
    this(qstr, localParams, params, req, jedis, 0);
  }

  RedisQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
          JedisCommands jedis, int maxJedisRetries) {
    super(qstr, localParams, params, req);
    this.jedis = jedis;

    String redisMethod = localParams.get("method");
    String redisKey = localParams.get("key");
    String operatorString = localParams.get("operator", "OR");

    if (redisMethod == null) {
      log.error("No method argument passed to RedisQParser.");
      throw new IllegalArgumentException("No method argument passed to RedisQParser.");
    } else if (!ALLOWED_METHODS.contains(redisMethod)) {
      log.error("Wrong Redis method: {}", redisMethod);
      throw new IllegalArgumentException("Wrong Redis method.");
    }

    if (redisKey == null || redisKey.isEmpty()) {
      log.error("No key argument passed to RedisQParser");
      throw new IllegalArgumentException("No key argument passed to RedisQParser");
    }

    if (operatorString != null && "AND".equalsIgnoreCase(operatorString)) {
      operator = BooleanClause.Occur.MUST;
    } else {
      operator = BooleanClause.Occur.SHOULD;
    }

    fetchDataFromRedis(redisMethod, redisKey, maxJedisRetries);
  }

  @Override
  public Query parse() throws SyntaxError {
    String fieldName = localParams.get(QueryParsing.V);
    BooleanQuery booleanQuery = new BooleanQuery(true);
    int booleanClausesTotal = 0;

    log.debug("Preparing a query for " + redisObjectsCollection.size() + " redis objects for field: " + fieldName);

    for (String termString : redisObjectsCollection) {
      try {
        TokenStream tokenStream = req.getSchema().getQueryAnalyzer().tokenStream(fieldName, termString);
        BytesRef term = new BytesRef();
        if (tokenStream != null) {
          CharTermAttribute charAttribute = tokenStream.addAttribute(CharTermAttribute.class);
          tokenStream.reset();

          int counter = 0;
          while (tokenStream.incrementToken()) {

            log.trace("Taking {} token from query string from {} for field: ",
                    ++counter, termString, fieldName);

            term = new BytesRef(charAttribute);
            TermQuery termQuery = new TermQuery(new Term(fieldName, term));
            booleanQuery.add(termQuery, this.operator);
            ++booleanClausesTotal;
          }

          tokenStream.end();
          tokenStream.close();
        } else {
          term.copyChars(termString);
          TermQuery termQuery = new TermQuery(new Term(fieldName, term));
          booleanQuery.add(termQuery, this.operator);
          ++booleanClausesTotal;
        }
      } catch (IOException ex) {
        log.error("Error occured during processing token stream.", ex);
      }
    }

    log.debug("Prepared a query for field {} with {} boolean clauses", booleanClausesTotal);

    return booleanQuery;
  }

  private void fetchDataFromRedis(String redisMethod, String redisKey, int maxJedisRetries) {
    int retries = 0;
    while (redisObjectsCollection == null && retries++ < maxJedisRetries + 1) {
      try {
        if (redisMethod.compareToIgnoreCase("smembers") == 0) {
          log.debug("Fetching smembers from Redis for key: " + redisKey);
          redisObjectsCollection = jedis.smembers(redisKey);
        }
      } catch (Exception ex) {
        log.warn("There was an error fetching data from redis.", ex);
      }
    }
  }
}
