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
import com.sematext.solr.redis.command.ValueFilter;
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
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.BoostQuery;
import org.apache.solr.common.util.Pair;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

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

    commands.put("GET", new Get(new ValueFilter()));
    commands.put("MGET", new MGet());
    commands.put("KEYS", new Keys());

    commands.put("SORT", new Sort());

    commands.put("EVAL", new Eval());
    commands.put("EVALSHA", new EvalSha());
  }

  /**
   * Jedis command handler
   */
  private final CommandHandler commandHandler;

  /**
   * Operator used to build query.
   */
  private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;

  /**
   * Redis command name to use.
   */
  private final String redisCommand;

  /**
   * Query tag name - virtual field name. Used for highlighting.
   */
  private final String queryTag;

  /**
   * Parameters which determines if this QParser should analyze data from Redis.
   */
  private final boolean useQueryTimeAnalyzer;

  /**
   * Parameters which determines whether score should be ignored or not.
   */
  private final boolean ignoreScore;

  /**
   *
   * @param qstr Query string
   * @param localParams Local parameters for this query parser
   * @param params Parameters
   * @param req Request object
   * @param commandHandler Redis command handler
   */
  RedisQParser(final String qstr, final SolrParams localParams, final SolrParams params, final SolrQueryRequest req,
          final CommandHandler commandHandler) {
    super(qstr, localParams, params, req);
    this.commandHandler = commandHandler;

    redisCommand = localParams.get("command") == null ? null : localParams.get("command").toUpperCase();
    ignoreScore = localParams.get("ignoreScore") == null ? false : Boolean.parseBoolean(localParams.get("ignoreScore"));
    final String operatorString = localParams.get("operator");
    queryTag = localParams.get("tag");

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

    useQueryTimeAnalyzer = localParams.getBool("useAnalyzer", false);
  }

  @Override
  public Query parse() throws SyntaxError {
    final String fieldName = localParams.get(QueryParsing.V);
    final List<Pair<BytesRef, Float>> queryTerms = new ArrayList<>();
    int booleanClausesTotal = 0;
    boolean shouldUseTermsQuery = (this.operator == BooleanClause.Occur.SHOULD) && ignoreScore;
    Float score = null;

    final Map<String, Float> results = commandHandler.executeCommand(commands.get(redisCommand), localParams);

    if (results != null) {
      log.debug("Preparing a query for {} redis objects for field: {}", results.size(), fieldName);

      for (final Map.Entry<String, Float> entry : results.entrySet()) {
        try {
          final String termString = entry.getKey();
          if (termString == null) {
            continue;
          }

          final Float newScore = entry.getValue();
          if (score != null && newScore != score && !ignoreScore) {
            shouldUseTermsQuery = false;
          }
          score = newScore;

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
                queryTerms.add(new Pair<>(new BytesRef(charAttribute), score));
                ++booleanClausesTotal;
              }
              tokenStream.end();
            }
          } else {
            queryTerms.add(new Pair<>(new BytesRef(termString), score));
            ++booleanClausesTotal;
          }
        } catch (final IOException ex) {
          log.error("Error occurred during processing token stream.", ex);
        }
      }
    }

    Query termsQuery = null;

    FieldType ft = null;
    final IndexSchema schema = req.getSchema();
    if (schema != null) {
      ft = schema.getFieldTypeNoEx(fieldName);
    }

    if (ft != null && ft.isPointField()) {
      log.trace("Using boolean query with numeric field subclauses (request params: {})", req.getParamString());

      final BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
      for (Pair<BytesRef, Float> pair : queryTerms) {
        addNumericTermToQuery(booleanQueryBuilder, fieldName, pair.first(), pair.second(), ft);
      }
      termsQuery = booleanQueryBuilder.build();
    } else {
      if (shouldUseTermsQuery) {
        log.trace("Using TermInSetQuery (request params: {})", req.getParamString());

        final List<BytesRef> terms = new ArrayList<>(queryTerms.size());
        for (Pair<BytesRef, Float> pair : queryTerms) {
          terms.add(pair.first());
        }
        termsQuery = new TermInSetQuery(fieldName, terms);
      } else {
        log.trace("Using boolean query with Terms clauses (request params: {})", req.getParamString());

        final BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (Pair<BytesRef, Float> pair : queryTerms) {
          addTermToQuery(booleanQueryBuilder, fieldName, pair.first(), pair.second());
        }
        termsQuery = booleanQueryBuilder.build();
      }
    }

    log.debug("Prepared a query for field {} with {} boolean clauses. (request params: {}}", fieldName,
        booleanClausesTotal, req.getParamString());

    if (queryTag == null || queryTag.isEmpty()) {
      return termsQuery;
    } else {
      return new TaggedQuery(termsQuery, queryTag);
    }
  }

  /**
   * Adds clause to query.
   *
   * @param queryBuilder Boolean query builder object which should take new clauses.
   * @param fieldName Field name used in added clause.
   * @param term Term
   * @param score Optional score
   */
  private void addTermToQuery(final BooleanQuery.Builder queryBuilder, final String fieldName, final BytesRef term,
      final Float score) {
    Query termQuery = new TermQuery(new Term(fieldName, term));
    if (!score.isNaN() && (score > 0)) {
      termQuery = new BoostQuery(termQuery, score);
    }

    queryBuilder.add(termQuery, this.operator);
  }

  /**
   * Adds numeric clause to query.
   *
   * @param queryBuilder Boolean query builder object which should take new clauses.
   * @param fieldName Field name used in added clause.
   * @param term Term
   * @param score Optional score
   * @param ft field type for the field we're using
   */
  private void addNumericTermToQuery(final BooleanQuery.Builder queryBuilder, final String fieldName,
      final BytesRef term, final Float score, final FieldType ft) {

    final String value = term.utf8ToString();
    Query clause = ft.getFieldQuery(this, req.getSchema().getField(fieldName), value);

    if (!score.isNaN() && (score > 0)) {
      clause = new BoostQuery(clause, score);
    }

    queryBuilder.add(clause, this.operator);
  }
}

