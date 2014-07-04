package com.sematext.solr.redis;

import com.sematext.solr.redis.command.Command;
import com.sematext.solr.redis.command.Get;
import com.sematext.solr.redis.command.HGet;
import com.sematext.solr.redis.command.HKeys;
import com.sematext.solr.redis.command.HMGet;
import com.sematext.solr.redis.command.HVals;
import com.sematext.solr.redis.command.Keys;
import com.sematext.solr.redis.command.LIndex;
import com.sematext.solr.redis.command.LRange;
import com.sematext.solr.redis.command.MGet;
import com.sematext.solr.redis.command.SDiff;
import com.sematext.solr.redis.command.SInter;
import com.sematext.solr.redis.command.SMembers;
import com.sematext.solr.redis.command.SRandMember;
import com.sematext.solr.redis.command.SUnion;
import com.sematext.solr.redis.command.Sort;
import com.sematext.solr.redis.command.ZRange;
import com.sematext.solr.redis.command.ZRangeByScore;
import com.sematext.solr.redis.command.ZRevRange;
import com.sematext.solr.redis.command.ZRevrangeByScore;
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
final class RedisQParser extends QParser {
  private static final Logger log = LoggerFactory.getLogger(RedisQParser.class);

  private static final Map<String, Command<?>> commands;
  static {
    commands = new HashMap<>();
    commands.put("SDIFF", new SDiff());
    commands.put("SINTER", new SInter());
    commands.put("SMEMBERS", new SMembers());
    commands.put("SRANDMEMBER", new SRandMember());
    commands.put("SUNION", new SUnion());

    commands.put("ZRANGE", new ZRange());
    commands.put("ZREVRANGE", new ZRevRange());
    commands.put("ZRANGEBYSCORE", new ZRangeByScore());
    commands.put("ZREVRANGEBYSCORE", new ZRevrangeByScore());

    commands.put("HGET", new HGet());
    commands.put("HKEYS", new HKeys());
    commands.put("HMGET", new HMGet());
    commands.put("HVALS", new HVals());

    commands.put("LRANGE", new LRange());
    commands.put("LINDEX", new LIndex());

    commands.put("GET", new Get());
    commands.put("MGET", new MGet());
    commands.put("KEYS", new Keys());

    commands.put("SORT", new Sort());
  }

  private final JedisPool jedisPool;
  private Map<String, Float> results;
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;
  private final String redisCommand;
  private final String redisKey;
  private final boolean useQueryTimeAnalyzer;
  private final int maxJedisRetries;

  RedisQParser(final String qstr, final SolrParams localParams, final SolrParams params, final SolrQueryRequest req,
    final JedisPool jedisPool) {
    this(qstr, localParams, params, req, jedisPool, 0);
  }

  RedisQParser(final String qstr, final SolrParams localParams, final SolrParams params, final SolrQueryRequest req,
          final JedisPool jedisPool, final int maxJedisRetries) {
    super(qstr, localParams, params, req);
    this.jedisPool = jedisPool;

    redisCommand = localParams.get("command") == null ? null : localParams.get("command").toUpperCase();
    redisKey = localParams.get("key");
    final String operatorString = localParams.get("operator");

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

    operator = "AND".equalsIgnoreCase(operatorString) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;

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

      for (final Map.Entry<String, Float> entry : results.entrySet()) {
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
        } catch (final IOException ex) {
          log.error("Error occurred during processing token stream.", ex);
        }
      }
    }

    log.debug("Prepared a query for field {} with {} boolean clauses", fieldName, booleanClausesTotal);

    return booleanQuery;
  }

  private void fetchDataFromRedis(final String redisCommand, final String redisKey, final int maxJedisRetries) {
    int retries = 0;
    final Command command = commands.get(redisCommand);

    while (results == null && retries++ < maxJedisRetries + 1) {
      Jedis jedis = null;
      try {
        jedis = jedisPool.getResource();
        results = command.execute(jedis, redisKey, localParams);
        jedisPool.returnResource(jedis);
      } catch (final JedisException ex) {
        jedisPool.returnBrokenResource(jedis);
        log.debug("There was an error fetching data from redis. Retrying", ex);
        if (retries >= maxJedisRetries + 1) {
          throw ex;
        }
      }
    }
  }
}
