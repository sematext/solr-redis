package com.sematext.solr.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.sematext.solr.redis.command.Command;
import com.sematext.solr.redis.command.Smembers;
import com.sematext.solr.redis.command.Zrevrangebyscore;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * RedisQParser is responsible for preparing a query based on data fetched from Redis.
 */
public class RedisQParser extends QParser {
  private static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  private static final Map<String, Command> commands;
  static {
    commands = new HashMap<>();
    commands.put("smembers", new Smembers());
    commands.put("zrevrangebyscore", new Zrevrangebyscore());
  }

  private final JedisPool jedisPool;
  private Map<String, Float> results = null;
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;
  private String redisCommand;
  private String redisKey;
  private boolean useQueryTimeAnalyzer;
  private int maxJedisRetries;

  RedisQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, JedisPool jedisPool) {
    this(qstr, localParams, params, req, jedisPool, 0);
  }

  RedisQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
          JedisPool jedisPool, int maxJedisRetries) {
    super(qstr, localParams, params, req);
    this.jedisPool = jedisPool;

    redisCommand = localParams.get("command");
    redisKey = localParams.get("key");
    String operatorString = localParams.get("operator");
    String useAnalyzerParam = localParams.get("useAnalyzer");

    if (redisCommand == null) {
      log.error("No command argument passed to RedisQParser.");
      throw new IllegalArgumentException("No command argument passed to RedisQParser.");
    } else if (!commands.containsKey(redisCommand)) {
      log.error("Wrong Redis command: {}", redisCommand);
      throw new IllegalArgumentException(String.format("Wrong Redis command '%s'.", redisCommand));
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

    useQueryTimeAnalyzer = useAnalyzerParam == null || Boolean.parseBoolean(useAnalyzerParam);

    this.maxJedisRetries = maxJedisRetries;
  }

  @Override
  public Query parse() throws SyntaxError {
    String fieldName = localParams.get(QueryParsing.V);
    BooleanQuery booleanQuery = new BooleanQuery(true);
    int booleanClausesTotal = 0;

    fetchDataFromRedis(redisCommand, redisKey, maxJedisRetries);

    if (results != null) {
      log.debug("Preparing a query for {} redis objects for field: {}", results.size(), fieldName);

      for (Map.Entry<String, Float> entry : results.entrySet()) {
        try {
          TokenStream tokenStream;
          String termString = entry.getKey();
          Float score = entry.getValue();

          if (useQueryTimeAnalyzer) {
            tokenStream = req.getSchema().getQueryAnalyzer().tokenStream(fieldName, termString);
          } else {
            tokenStream = new KeywordAnalyzer().tokenStream(fieldName, termString);
          }

          BytesRef term = new BytesRef();
          if (tokenStream != null) {
            CharTermAttribute charAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            int counter = 0;
            while (tokenStream.incrementToken()) {

              log.trace("Taking {} token from query string from {} for field: {}", ++counter, termString, fieldName);

              term = new BytesRef(charAttribute);
              TermQuery termQuery = new TermQuery(new Term(fieldName, term));
              if (score != Float.NaN) {
                termQuery.setBoost(score);
              }
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
          log.error("Error occurred during processing token stream.", ex);
        }
      }
    }

    log.debug("Prepared a query for field {} with {} boolean clauses", fieldName, booleanClausesTotal);

    return booleanQuery;
  }

  private void fetchDataFromRedis(String redisCommand, String redisKey, int maxJedisRetries) {
    int retries = 0;
    final Command command = commands.get(redisCommand);

    while (results == null && retries++ < maxJedisRetries + 1) {
      Jedis jedis = null;
      try {
        jedis = jedisPool.getResource();
        results = command.execute(jedis, redisKey, localParams);
        jedisPool.returnResource(jedis);
      } catch (JedisException ex) {
        jedisPool.returnBrokenResource(jedis);
        log.debug("There was an error fetching data from redis. Retrying", ex);
        if (retries >= maxJedisRetries + 1) {
          throw ex;
        }
      }
    }
  }
}
