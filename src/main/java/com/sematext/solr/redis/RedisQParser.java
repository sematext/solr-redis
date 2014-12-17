package com.sematext.solr.redis;

import com.sematext.lucene.query.TaggedQuery;
import com.sematext.solr.redis.command.Command;
import com.sematext.solr.redis.command.Eval;
import com.sematext.solr.redis.command.EvalSha;
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
 *
 * @author prog
 * @author lstrojny
 */
final class RedisQParser extends QParser {
  /**
   * Logger
   */
  private static final Logger log = LoggerFactory.getLogger(RedisQParser.class);

  /**
   * Collection of commands
   */
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

    commands.put("EVAL", new Eval());
    commands.put("EVALSHA", new EvalSha());
  }

  /**
   * Jedis pool object.
   */
  private final JedisPool jedisPool;

  /**
   * Results of redis command.
   */
  private Map<String, Float> results;

  /**
   * Operator used to build query.
   */
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;

  /**
   * Redis command name to use.
   */
  private final String redisCommand;

  /**
   * Field alias name - virtual field name. Used for highlighting.
   */
  private final String fieldAlias;

  /**
   * Parameters which determines if this QParser should analyze data from Redis.
   */
  private final boolean useQueryTimeAnalyzer;

  /**
   * Parameter which determines how many times Redis operation should be retried before throwing an exception.
   */
  private final int maxJedisRetries;

  /**
   *
   * @param qstr Query string
   * @param localParams Local parameters for this query parser
   * @param params Parameters
   * @param req Request object
   * @param jedisPool Jedis pool which should be used to connect to Redis.
   */
  RedisQParser(final String qstr, final SolrParams localParams, final SolrParams params, final SolrQueryRequest req,
    final JedisPool jedisPool) {
    this(qstr, localParams, params, req, jedisPool, 0);
  }

  /**
   *
   * @param qstr Query string
   * @param localParams Local parameters for this query parser
   * @param params Parameters
   * @param req Request object
   * @param jedisPool Jedis pool which should be used to connect to Redis.
   * @param maxJedisRetries Parameter which determines how many times Redis operation should be retried
   */
  RedisQParser(final String qstr, final SolrParams localParams, final SolrParams params, final SolrQueryRequest req,
          final JedisPool jedisPool, final int maxJedisRetries) {
    super(qstr, localParams, params, req);
    this.jedisPool = jedisPool;

    redisCommand = localParams.get("command") == null ? null : localParams.get("command").toUpperCase();
    final String operatorString = localParams.get("operator");
    fieldAlias = localParams.get("field_alias");

    if (redisCommand == null) {
      log.error("No command argument passed to RedisQParser.");
      throw new IllegalArgumentException("No command argument passed to RedisQParser.");
    }
    else if (!commands.containsKey(redisCommand))
    {
      log.error("Wrong Redis command: {}", redisCommand);
      throw new IllegalArgumentException(String.format("Wrong Redis command '%s'.", redisCommand));
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

    fetchDataFromRedis(redisCommand, maxJedisRetries);

    if (results != null) {
      log.debug("Preparing a query for {} redis objects for field: {}", results.size(), fieldName);

      for (final Map.Entry<String, Float> entry : results.entrySet()) {
        try {
          final String termString = entry.getKey();
          if (termString == null) {
            continue;
          }

          final Float score = entry.getValue();

          if (useQueryTimeAnalyzer) {

            log.trace("Term string {}", termString);

            try (final TokenStream tokenStream =
                req.getSchema().getQueryAnalyzer().tokenStream(fieldName, termString)) {
              final CharTermAttribute charAttribute = tokenStream.addAttribute(CharTermAttribute.class);
              tokenStream.reset();

              int counter = 0;
              while (tokenStream.incrementToken()) {

                log.trace("Taking {} token {} with score {} from query string from {} for field: {}", ++counter,
                    charAttribute, score, termString, fieldName);

                addTermToQuery(booleanQuery, fieldName, new BytesRef(charAttribute), score);
                ++booleanClausesTotal;
              }

              tokenStream.end();
            }
          } else {
            addTermToQuery(booleanQuery, fieldName, new BytesRef(termString), score);
            ++booleanClausesTotal;
          }
        } catch (final IOException ex) {
          log.error("Error occurred during processing token stream.", ex);
        }
      }
    }

    log.debug("Prepared a query for field {} with {} boolean clauses", fieldName, booleanClausesTotal);

    if (fieldAlias == null || fieldAlias.isEmpty()) {
      return booleanQuery;
    } else {
      return new TaggedQuery(booleanQuery, fieldAlias);
    }
  }

  /**
   * Adds clause to query.
   *
   * @param query Boolean query object which should take new clauses.
   * @param fieldName Field name used in added clause.
   * @param term Term
   * @param score Optional score
   */
  private void addTermToQuery(final BooleanQuery query, final String fieldName, final BytesRef term,
      final Float score) {
    final TermQuery termQuery = new TermQuery(new Term(fieldName, term));

    if (!score.isNaN()) {
      termQuery.setBoost(score);
    }

    query.add(termQuery, this.operator);
  }

  /**
   *
   * @param redisCommand Redus command
   * @param maxRetries Maximum retries of Redis command
   */
  private void fetchDataFromRedis(final String redisCommand, final int maxRetries) {
    int retries = 0;
    final Command command = commands.get(redisCommand);

    while (results == null && retries++ < maxRetries + 1) {
      Jedis jedis = null;
      try {
        jedis = jedisPool.getResource();
        results = command.execute(jedis, localParams);
        jedisPool.returnResource(jedis);
      } catch (final JedisException ex) {
        jedisPool.returnBrokenResource(jedis);
        log.debug("There was an error fetching data from redis. Retrying", ex);
        if (retries >= maxRetries + 1) {
          throw ex;
        }
      }
    }
  }
}
