package com.sematext.solr.redis;

import com.sematext.solr.redis.command.Command;
import com.sematext.solr.redis.command.GET;
import com.sematext.solr.redis.command.HGET;
import com.sematext.solr.redis.command.HKEYS;
import com.sematext.solr.redis.command.HMGET;
import com.sematext.solr.redis.command.HVALS;
import com.sematext.solr.redis.command.KEYS;
import com.sematext.solr.redis.command.LINDEX;
import com.sematext.solr.redis.command.LRANGE;
import com.sematext.solr.redis.command.MGET;
import com.sematext.solr.redis.command.SDIFF;
import com.sematext.solr.redis.command.SINTER;
import com.sematext.solr.redis.command.SMEMBERS;
import com.sematext.solr.redis.command.SORT;
import com.sematext.solr.redis.command.SRANDMEMBER;
import com.sematext.solr.redis.command.SUNION;
import com.sematext.solr.redis.command.ZRANGEBYSCORE;
import com.sematext.solr.redis.command.ZREVRANGEBYSCORE;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RedisQParser is responsible for preparing a query based on data fetched from Redis.
 */
public class RedisQParser extends QParser {
  private static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  private static final Map<String, Command> commands;
  static {
    commands = new HashMap<>();
    commands.put("SDIFF", new SDIFF());
    commands.put("SINTER", new SINTER());
    commands.put("SMEMBERS", new SMEMBERS());
    commands.put("SRANDMEMBER", new SRANDMEMBER());
    commands.put("SUNION", new SUNION());

    commands.put("ZRANGEBYSCORE", new ZRANGEBYSCORE());
    commands.put("ZREVRANGEBYSCORE", new ZREVRANGEBYSCORE());

    commands.put("HGET", new HGET());
    commands.put("HKEYS", new HKEYS());
    commands.put("HMGET", new HMGET());
    commands.put("HVALS", new HVALS());

    commands.put("LRANGE", new LRANGE());
    commands.put("LINDEX", new LINDEX());

    commands.put("GET", new GET());
    commands.put("MGET", new MGET());
    commands.put("KEYS", new KEYS());

    commands.put("SORT", new SORT());
  }

  private final JedisPool jedisPool;
  private Map<String, Float> results = null;
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;
  private final String redisCommand;
  private final String redisKey;
  private final boolean useQueryTimeAnalyzer;
  private final int maxJedisRetries;

  RedisQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, JedisPool jedisPool) {
    this(qstr, localParams, params, req, jedisPool, 0);
  }

  RedisQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
          JedisPool jedisPool, int maxJedisRetries) {
    super(qstr, localParams, params, req);
    this.jedisPool = jedisPool;

    redisCommand = localParams.get("command") == null ? null : localParams.get("command").toUpperCase();
    redisKey = localParams.get("key");
    String operatorString = localParams.get("operator");

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

    useQueryTimeAnalyzer = localParams.getBool("useAnalyzer", true);

    this.maxJedisRetries = maxJedisRetries;
  }

  @Override
  public Query parse() throws SyntaxError {
    final String fieldName = localParams.get(QueryParsing.V);
    final BooleanQuery booleanQuery = new BooleanQuery(true);
    int booleanClausesTotal = 0;

    fetchDataFromRedis(redisCommand, redisKey, maxJedisRetries);

    if (results != null) {
      log.debug("Preparing a query for {} redis objects for field: {}", results.size(), fieldName);

      for (Map.Entry<String, Float> entry : results.entrySet()) {
        try {
          final TokenStream tokenStream;
          final String termString = entry.getKey();
          if (termString == null) {
            continue;
          }

          final Float score = entry.getValue();

          if (useQueryTimeAnalyzer) {
            tokenStream = req.getSchema().getQueryAnalyzer().tokenStream(fieldName, termString);

            final CharTermAttribute charAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            int counter = 0;
            while (tokenStream.incrementToken()) {

              log.trace("Taking {} token {} with score {} from query string from {} for field: {}", ++counter,
                  new String(charAttribute.buffer()), score, termString, fieldName);

              final TermQuery termQuery = new TermQuery(new Term(fieldName, new BytesRef(charAttribute)));
              if (!score.isNaN()) {
                termQuery.setBoost(score);
              }
              booleanQuery.add(termQuery, this.operator);
              ++booleanClausesTotal;
            }

            tokenStream.end();
            tokenStream.close();
          } else {
            final TermQuery termQuery = new TermQuery(new Term(fieldName, new BytesRef(termString)));
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
