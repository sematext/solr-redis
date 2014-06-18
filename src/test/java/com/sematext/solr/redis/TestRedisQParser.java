package com.sematext.solr.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestRedisQParser {

  private QParser redisQParser;

  @Mock
  private SolrParams localParamsMock;

  @Mock
  private SolrParams paramsMock;

  @Mock
  private SolrQueryRequest requestMock;

  @Mock
  private IndexSchema schema;

  @Mock
  private JedisPool jedisPoolMock;

  @Mock
  private Jedis jedisMock;


  @Before
  public void setUp() {
    initMocks(this);
    when(jedisPoolMock.getResource()).thenReturn(jedisMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingCommand() {
    when(localParamsMock.get(any(String.class))).thenReturn(null);
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingKey() {
    when(localParamsMock.get("command")).thenReturn("smembers");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
  }

  @Test
  public void shouldQueryRedisOnSmembersCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldAddTermsFromRedisOnSmembersCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).smembers("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSmembers() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).smembers("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnRangeByScoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(any(String.class), any(String.class), any(String.class))).
            thenReturn(new HashSet<>(Arrays.asList(
                    new Tuple("123", (double)1.0f), new Tuple("321", (double)1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).zrevrangeByScoreWithScores("simpleKey", "+inf", "-inf");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

    @Test
  public void shouldAddTermsFromRedisOnRangeByScoreCommdnWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("min")).thenReturn("1");
    when(localParamsMock.get("max")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(any(String.class), any(String.class), any(String.class))).
            thenReturn(new HashSet<>(Arrays.asList(
                new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).zrevrangeByScoreWithScores("simpleKey", "100", "1");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnRevrangeByScoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(any(String.class), any(String.class), any(String.class))).
            thenReturn(new HashSet<>(Arrays.asList(
                                    new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).zrevrangeByScoreWithScores("simpleKey", "+inf", "-inf");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldTurnAnalysisOff() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("useAnalyzer")).thenReturn("false");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).smembers("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldTurnAnalysisOn() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("useAnalyzer")).thenReturn("true");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new WhitespaceAnalyzer(Version.LUCENE_48));
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).smembers("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(3, terms.size());
  }

  @Test
  public void shouldRetryWhenRedisFailed() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("useAnalyzer")).thenReturn("true");
    when(localParamsMock.get("retries")).thenReturn("2");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new WhitespaceAnalyzer(Version.LUCENE_48));
    when(jedisPoolMock.getResource()).thenReturn(new JedisStub(JedisStub.Action.EXCEPTION, JedisStub.Action.TERM));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock, 2);
    Query query = redisQParser.parse();
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  private static class JedisStub extends Jedis{
    public enum Action {
      EXCEPTION,
      TERM
    }

    private final Action [] actions;
    private int counter;

    public JedisStub(Action ... actions) {
      super("localhost");
      counter = 0;
      this.actions = actions;
    }

    @Override
    public Set<String> smembers(String key) {
      try {
        if (actions[counter] == Action.TERM) {
          return new HashSet<>(Arrays.asList("term"));
        } if (actions[counter] == Action.EXCEPTION) {
          throw new JedisConnectionException("");
        }
      } finally {
        counter++;
      }
      return null;
    }
  }
}
