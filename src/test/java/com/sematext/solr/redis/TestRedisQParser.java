package com.sematext.solr.redis;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
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
import redis.clients.jedis.exceptions.JedisException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPOutputStream;
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

  @Mock
  private Jedis jedisFailingMock;

  private CommandHandler commandHandler;

  @Before
  public void setUp() {
    initMocks(this);
    when(jedisPoolMock.getResource()).thenReturn(jedisMock);
    commandHandler = new RetryingCommandHandler(jedisPoolMock, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingCommand() {
    when(localParamsMock.get(anyString())).thenReturn(null);
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingKey() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    redisQParser.parse();
  }

  @Test
  public void shouldQueryRedisOnSmembersCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSmembers() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldQueryRedisOnSrandmemberCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).srandmember("simpleKey", 2);
  }

  @Test
  public void shouldAddMultipleTermsFromRedisOnSrandmemberCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.srandmember(anyString(), anyInt())).thenReturn(Collections.singletonList("123"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).srandmember("simpleKey", 1);
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSrandmember() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("srandmember");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.srandmember(anyString(), anyInt())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).srandmember("simpleKey", 1);
  }

  @Test
  public void shouldQueryRedisOnSinterCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sinter");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).sinter("key1", "key2");
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).sinter("key1", "key2");
  }

  @Test
  public void shouldQueryRedisOnKeysCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("keys");
    when(localParamsMock.get("key")).thenReturn("pattern");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).keys("pattern");
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfKeys() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("keys");
    when(localParamsMock.get("key")).thenReturn("pattern");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sdiff(anyString(), anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).keys("pattern");
  }

  @Test
  public void shouldQueryRedisOnSdiffCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sdiff");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).sdiff("key1", "key2");
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).sdiff("key1", "key2");
  }

  @Test
  public void shouldQueryRedisOnSunionCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("sunion");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).sunion("key1", "key2");
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).sunion("key1", "key2");
  }

  @Test
  public void shouldQueryRedisOnHvalsCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hvals");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).hvals("simpleKey");
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHvals() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hvals");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hvals(anyString())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).hvals("simpleKey");
  }

  @Test
  public void shouldQueryRedisOnHkeysCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hkeys");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).hkeys("simpleKey");
    final Set<Term> terms = new HashSet<>();
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHkeys() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hkeys");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hkeys(anyString())).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).hkeys("simpleKey");
  }

  @Test
  public void shouldQueryRedisOnHgetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("field")).thenReturn("f1");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    redisQParser.parse();
    verify(jedisMock).hget("simpleKey", "f1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionIfFieldIsMissing() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).hget("simpleKey", "f1");
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfHget() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("hget");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("field")).thenReturn("f1");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.hget(anyString(), anyString())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).hget("simpleKey", "f1");
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
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).hmget("hash", "field1");
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).hmget("hash", "field1");
  }

  @Test
  public void shouldQueryRedisOnGetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    redisQParser.parse();
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldAddTermsFromRedisOnGetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(any(byte[].class))).thenReturn("val".getBytes());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldParseJsonTermsFromRedisOnGetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("serialization")).thenReturn("json");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(any(byte[].class))).thenReturn("[1,2,3]".getBytes());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(3, redisQParser.parse());
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldDeflateGzipTermsFromRedisOnGetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("compression")).thenReturn("gzip");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(any(byte[].class))).thenReturn(Compressor.compressGzip("1".getBytes()));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldDeflateGzipAndParseJsonTermsFromRedisOnGetCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("compression")).thenReturn("gzip");
    when(localParamsMock.get("serialization")).thenReturn("json");

    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(any(byte[].class))).thenReturn(Compressor.compressGzip("[100,200,300]".getBytes()));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(3, redisQParser.parse());
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfGet() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("get");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.get(anyString())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).get("simpleKey".getBytes());
  }

  @Test
  public void shouldAddTermsFromRedisOnLindexCommandDefault0() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn("value");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).lindex("simpleKey", 0);
  }

  @Test
  public void shouldAddTermsFromRedisOnLindexCommand() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("index")).thenReturn("10");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn("value");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).lindex("simpleKey", 10);
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfLindex() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lindex");
    when(localParamsMock.get("index")).thenReturn("10");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lindex(anyString(), anyLong())).thenReturn(null);
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).lindex("simpleKey", 10);
  }

  @Test
  public void shouldQueryRedisOnMgetCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("mget");
    when(localParamsMock.get("key")).thenReturn("key1");
    when(localParamsMock.get("keyfoo")).thenReturn("key3");
    when(localParamsMock.get("key1")).thenReturn("key2");
    when(localParamsMock.get("keyempty")).thenReturn("");
    when(localParamsMock.getParameterNamesIterator()).thenReturn(Arrays.asList("command", "key", "key1", "keyfoo", "keyempty").iterator());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).mget("key1", "key2");
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).mget("key1", "key2");
  }

  @Test
  public void shouldQueryRedisOnLrangeCommand() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, -1);
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandCustomMin() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("min")).thenReturn("-1");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", -1, -1);
  }

  @Test
  public void shouldAddTermsFromRedisOnLrangeCommandCustomMax() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("max")).thenReturn("1");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, 1);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 2, 3);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, -1);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, -1);
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, -1);
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfLrange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("lrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.lrange(anyString(), anyLong(), anyLong())).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    verify(jedisMock).lrange("simpleKey", 0, -1);
  }

  @Test
  public void shouldReturnEmptyQueryOnEmptyResultOfSort() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(new ArrayList<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(0, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams()), getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSort() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams()), getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortAlgorithmAlpha() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("algorithm")).thenReturn("alpha");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().alpha()), getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortOrderAsc() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("order")).thenReturn("asc");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().asc()), getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortOrderDesc() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("order")).thenReturn("desc");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().desc()), getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortLimit() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("limit")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(0, 100)),
        getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortOffset() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("offset")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(100, 0)),
        getSortingParamString(argument.getValue()));
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().limit(100, 1000)),
        getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromSortWithByClause() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("sort");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("by")).thenReturn("foo_*");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.sort(anyString(), any(SortingParams.class))).thenReturn(Arrays.asList("123", "321"));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().by("foo_*")),
        getSortingParamString(argument.getValue()));
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
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    final ArgumentCaptor<SortingParams> argument = ArgumentCaptor.forClass(SortingParams.class);
    verify(jedisMock).sort(eq("simpleKey"), argument.capture());
    Assert.assertEquals(getSortingParamString(new SortingParams().get("get_*")),
        getSortingParamString(argument.getValue()));
  }

  @Test
  public void shouldAddTermsFromRedisOnZrevrangeCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeWithScores(anyString(), anyLong(), anyLong()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrevrangeWithScores("simpleKey", 0, -1);
  }

  @Test
  public void shouldAddTermsFromRedisOnZrevrangeCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("range_start")).thenReturn("1");
    when(localParamsMock.get("range_end")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeWithScores(anyString(), anyLong(), anyLong()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrevrangeWithScores("simpleKey", 1, 100);
  }

  @Test
  public void shouldAddTermsFromRedisOnZrangeCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeWithScores(anyString(), anyLong(), anyLong()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrangeWithScores("simpleKey", 0, -1);
  }

  @Test
  public void shouldAddTermsFromRedisOnZrangeCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrange");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("range_start")).thenReturn("1");
    when(localParamsMock.get("range_end")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeWithScores(anyString(), anyLong(), anyLong()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrangeWithScores("simpleKey", 1, 100);
  }

  @Test
  public void shouldAddTermsFromRedisOnZrevrangebyscoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(anyString(), anyString(), anyString()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrevrangeByScoreWithScores("simpleKey", "+inf", "-inf");
  }

    @Test
  public void shouldAddTermsFromRedisOnZrevrangebyscoreCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrevrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("min")).thenReturn("1");
    when(localParamsMock.get("max")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrevrangeByScoreWithScores(anyString(), anyString(), anyString()))
      .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    when(requestMock.getSchema()).thenReturn(schema);
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrevrangeByScoreWithScores("simpleKey", "100", "1");
  }

  @Test
  public void shouldAddTermsFromRedisOnZrangebyscoreCommandWithDefaultParams() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeByScoreWithScores(anyString(), anyString(), anyString()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrangeByScoreWithScores("simpleKey", "-inf", "+inf");
  }

  @Test
  public void shouldAddTermsFromRedisOnZrangebyscoreCommandWithCustomRange() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("zrangebyscore");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("min")).thenReturn("1");
    when(localParamsMock.get("max")).thenReturn("100");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.zrangeByScoreWithScores(anyString(), anyString(), anyString()))
        .thenReturn(new HashSet<>(Arrays.asList(new Tuple("123", (double) 1.0f), new Tuple("321", (double) 1.0f))));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).zrangeByScoreWithScores("simpleKey", "1", "100");
  }

  @Test
  public void shouldAddTermsFromRedisOnEval() throws SyntaxError, IOException {
    when(localParamsMock.get("command")).thenReturn("eval");
    when(localParamsMock.get("script")).thenReturn("return 1;");
    when(localParamsMock.get("key")).thenReturn("k");
    when(localParamsMock.get("arg")).thenReturn("a");
    when(localParamsMock.getParameterNamesIterator())
        .thenReturn(
            Arrays.asList("command", "script", "key", "arg").iterator(),
            Arrays.asList("command", "script", "key", "arg").iterator());

    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.eval(anyString(), anyInt(), (String) anyVararg())).thenReturn(1);

    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer());
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(1, redisQParser.parse());
    verify(jedisMock).eval("return 1;", 1, "k", "a");
  }

  @Test
  public void shouldTurnAnalysisOff() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get("useAnalyzer")).thenReturn("false");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(2, redisQParser.parse());
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldTurnAnalysisOn() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.getBool("useAnalyzer", false)).thenReturn(true);
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new WhitespaceAnalyzer());
    when(jedisMock.smembers(anyString())).thenReturn(new HashSet<>(Arrays.asList("123 124", "321")));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, commandHandler);
    assertTermSize(3, redisQParser.parse());
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldRetryWhenRedisFailed() throws SyntaxError {
    when(localParamsMock.get("command")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.getBool("useAnalyzer", false)).thenReturn(false);
    when(localParamsMock.get("retries")).thenReturn("2");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new WhitespaceAnalyzer());
    when(jedisPoolMock.getResource()).thenReturn(jedisFailingMock).thenReturn(jedisMock);
    when(jedisFailingMock.smembers("simpleKey")).thenThrow(new JedisException("Synthetic exception"));
    when(jedisMock.smembers("simpleKey")).thenReturn(new HashSet<String>(Collections.singletonList("value")));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, new RetryingCommandHandler(jedisPoolMock, 1));
    assertTermSize(1, redisQParser.parse());
  }

  private static String getSortingParamString(final SortingParams params) {
    final StringBuilder builder = new StringBuilder();
    for (final byte[] param: params.getParams()) {
      builder.append(new String(param));
      builder.append(" ");
    }

    return builder.toString().trim();
  }

  private static void assertTermSize(final int size, final Query query) {
    final BooleanQuery booleanQuery = (BooleanQuery) query;
    Assert.assertEquals(size, booleanQuery.clauses().size());
  }
}

final class Compressor
{
  public static byte[] compressGzip(final byte[] input) {
    final byte[] compressed;
    try {
      try (
          ByteArrayOutputStream output = new ByteArrayOutputStream(input.length);
      ) {
        try (
            GZIPOutputStream stream = new GZIPOutputStream(output);
        ) {
          stream.write(input);
        }
        compressed = output.toByteArray();
      }

      return compressed;
    } catch (final IOException e) {
      return null;
    }
  }
}
