package com.sematext.solr.redis;

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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.*;
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
    when(localParamsMock.get(anyString())).thenReturn(null);
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
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
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
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<String>());
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
  public void shouldQueryRedisOnSrandmemberCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).srandmember("simpleKey", 1);
  }

  @Test
  public void shouldAddMultipleTermsFromRedisOnSrandmemberCommandWithExplicitCount() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("count")).thenReturn("2");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.srandmember(anyString(), anyInt())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).srandmember("simpleKey", 2);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddMultipleTermsFromRedisOnSrandmemberCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.srandmember(anyString(), anyInt())).thenReturn(Arrays.asList("123"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).srandmember("simpleKey", 1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSrandmember() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.srandmember(anyString(), anyInt())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).srandmember("simpleKey", 1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnSinterCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sinter");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).sinter("key1", "key2", "key3");
  }

  @Test
  public void shouldAddTermsFromRedisOnSinterCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sinter");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sinter(anyString(), anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sinter("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSinter() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sinter");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sinter(anyString(), anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sinter("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnKeysCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("keys");
    when(localParamsMock.get("key")).thenReturn("pattern");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).keys("pattern");
  }

  @Test
  public void shouldAddTermsFromRedisOnKeysCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("keys");
    when(localParamsMock.get("key")).thenReturn("pattern");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.keys(anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).keys("pattern");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfKeys() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("keys");
    when(localParamsMock.get("key")).thenReturn("pattern");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sdiff(anyString(), anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).keys("pattern");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnSdiffCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sdiff");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).sdiff("key1", "key2", "key3");
  }

  @Test
  public void shouldAddTermsFromRedisOnSdiffCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sdiff");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sdiff(anyString(), anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sdiff("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSdiff() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sdiff");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sdiff(anyString(), anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sdiff("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnSunionCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sunion");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).sunion("key1", "key2", "key3");
  }

  @Test
  public void shouldAddTermsFromRedisOnSunionCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sunion");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sunion(anyString(), anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sunion("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSunion() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sunion");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sunion(anyString(), anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).sunion("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnHvalsCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hvals");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).hvals("simpleKey");
  }

  @Test
  public void shouldAddTermsFromRedisOnHvalsCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hvals");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hvals(anyString())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hvals("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHvals() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hvals");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hvals(anyString())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hvals("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnHkeysCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hkeys");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).hkeys("simpleKey");
  }

  @Test
  public void shouldAddTermsFromRedisOnHkeysCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hkeys");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hkeys(anyString())).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hkeys("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHkeys() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hkeys");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hkeys(anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hkeys("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnHgetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("field")).thenReturn("f1");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).hget("simpleKey", "f1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionIfFieldIsMissing() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).hget("simpleKey", "f1");
  }

  @Test
  public void shouldAddTermsFromRedisOnHgetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("field")).thenReturn("f1");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hget(anyString(), anyString())).thenReturn("123");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hget("simpleKey", "f1");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHget() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("field")).thenReturn("f1");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hget(anyString(), anyString())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hget("simpleKey", "f1");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnHmgetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hmget");
    when(localParamsMock.get("key")).thenReturn("hash");
    when(localParamsMock.get("fieldfoo")).thenReturn("field2");
    when(localParamsMock.get("field1")).thenReturn("field1");
    when(localParamsMock.get("field")).thenReturn("field3");
    when(localParamsMock.get("fieldempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(
        Arrays.asList("command", "key", "field1", "fieldfoo", "fieldempty", "field").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).hmget("hash", "field1", "field2", "field3");
  }

  @Test
  public void shouldAddTermsFromRedisOnHmgetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hmget");
    when(localParamsMock.get("key")).thenReturn("hash");
    when(localParamsMock.get("field")).thenReturn("field1");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "field").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hmget(anyString(), anyString())).thenReturn(Arrays.asList("123"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hmget("hash", "field1");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHmget() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hmget");
    when(localParamsMock.get("key")).thenReturn("hash");
    when(localParamsMock.get("field")).thenReturn("field1");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "field").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hmget(anyString(), anyString())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).hmget("hash", "field1");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnGetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).get("simpleKey");
  }

  @Test
  public void shouldAddTermsFromRedisOnGetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(anyString())).thenReturn("value");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).get("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfGet() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(anyString())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).get("simpleKey");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLindexCommandDefault0() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn("value");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lindex("simpleKey", 0);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLindexCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("index")).thenReturn("10");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn("value");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lindex("simpleKey", 10);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(1, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfLindex() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("index")).thenReturn("10");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lindex("simpleKey", 10);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnMgetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("mget");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).mget("key1", "key2", "key3");
  }

  @Test
  public void shouldAddTermsFromRedisOnMgetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("mget");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.mget(anyString(), anyString())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).mget("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfMget() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("mget");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.mget(anyString(), anyString())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).mget("key1", "key2");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldQueryRedisOnLrangeCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandCustomMin() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn("-1");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", -1, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandCustomMax() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("max")).thenReturn("1");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, 1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandCustomMinAndMax() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn("2");
    when(localParamsMock.get("max")).thenReturn("3");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 2, 3);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandInvalidMinAndMaxFallsBackToDefault()
      throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn("-foo");
    when(localParamsMock.get("min")).thenReturn("-bar");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandEmptyStringMinAndMaxFallsBackToDefault()
      throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn(" ");
    when(localParamsMock.get("min")).thenReturn("   ");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandEmptyMinAndMaxFallsBackToDefault() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn("");
    when(localParamsMock.get("max")).thenReturn("");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfLrange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).lrange("simpleKey", 0, -1);
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfSort() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams()), getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }

  @Test
  public void shouldAddTermsFromSort() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams()), getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortAlgorithmAlpha() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("algorithm")).thenReturn("alpha");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().alpha()), getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortOrderAsc() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("order")).thenReturn("asc");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().asc()), getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortOrderDesc() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("order")).thenReturn("desc");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().desc()), getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortLimit() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("limit")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(0, 100)),
        getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortOffset() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("offset")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(100, 0)),
        getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortLimitAndOffset() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("offset")).thenReturn("100");
    when(localParamsMock.get("limit")).thenReturn("1000");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(100, 1000)),
        getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortWithByClause() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("by")).thenReturn("foo_*");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().by("foo_*")),
        getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromSortWithSingleGetClause() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("get")).thenReturn("get_*");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "get").iterator());
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().get("get_*")),
        getSortingParamString(argument.getValue()));
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnZrevrangebyscoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(anyString(), anyString(), anyString())).
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
  public void shouldAddTermsFromRedisOnZrevrangebyscoreCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("min")).thenReturn("1");
    when(localParamsMock.get("max")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(anyString(), anyString(), anyString())).
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
  public void shouldAddTermsFromRedisOnZrangebyscoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeByScoreWithScores(anyString(), anyString(), anyString())).
        thenReturn(new HashSet<>(Arrays.asList(
            new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).zrangeByScoreWithScores("simpleKey", "+inf", "-inf");
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }

  @Test
  public void shouldAddTermsFromRedisOnZrangebyscoreCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("min")).thenReturn("1");
    when(localParamsMock.get("max")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeByScoreWithScores(anyString(), anyString(), anyString())).
        thenReturn(new HashSet<>(Arrays.asList(
            new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisPoolMock);
    Query query = redisQParser.parse();
    verify(jedisMock).zrangeByScoreWithScores("simpleKey", "100", "1");
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
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
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
    when(localParamsMock.getBool("useAnalyzer", true)).thenReturn(true);
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new WhitespaceAnalyzer(Version.LUCENE_48));
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
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
    when(localParamsMock.getBool("useAnalyzer", true)).thenReturn(false);
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

  private static String getSortingParamString(SortingParams params) {
    StringBuilder builder = new StringBuilder();
    for (byte[] param: params.getParams()) {
      builder.append(new String(param));
      builder.append(" ");
    }

    return builder.toString().trim();
  }
}
