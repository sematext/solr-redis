package com.sematext.solr.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import redis.clients.jedis.Jedis;

public class TestRedisQParser {
  
  private RedisQParser redisQParser;

  @Mock
  private SolrParams localParamsMock;

  @Mock
  private SolrParams paramsMock;

  @Mock
  private SolrQueryRequest requestMock;

  @Mock
  private IndexSchema schema;

  @Mock
  private Jedis jedisMock;


  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingMethod() {
    when(localParamsMock.get(any(String.class))).thenReturn(null);
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnMissingKey() {
    when(localParamsMock.get("method")).thenReturn("smembers");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisMock);
  }

  @Test
  public void shouldQueryRedisOnSmembersMethod() {
    when(localParamsMock.get("method")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisMock);
    verify(jedisMock).smembers("simpleKey");
  }

  @Test
  public void shouldAddTermsFromRedisOnSmembersMethod() throws SyntaxError, IOException {
    when(localParamsMock.get("method")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<>(Arrays.asList("123", "321")));
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisMock);
    verify(jedisMock).smembers("simpleKey");
    Query query = redisQParser.parse();
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(2, terms.size());
  }
 
  @Test
  public void shouldReturnEmptyQueryOnEmptyListOfSmembers() throws SyntaxError, IOException {
    when(localParamsMock.get("method")).thenReturn("smembers");
    when(localParamsMock.get("key")).thenReturn("simpleKey");
    when(localParamsMock.get(QueryParsing.V)).thenReturn("string_field");
    when(jedisMock.smembers(any(String.class))).thenReturn(new HashSet<String>());
    when(requestMock.getSchema()).thenReturn(schema);
    when(schema.getQueryAnalyzer()).thenReturn(new StandardAnalyzer(Version.LUCENE_48));
    redisQParser = new RedisQParser("string_field", localParamsMock, paramsMock, requestMock, jedisMock);
    verify(jedisMock).smembers("simpleKey");
    Query query = redisQParser.parse();
    Set<Term> terms = new HashSet<>();
    query.extractTerms(terms);
    Assert.assertEquals(0, terms.size());
  }
}
