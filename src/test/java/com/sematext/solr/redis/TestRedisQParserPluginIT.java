package com.sematext.solr.redis;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class TestRedisQParserPluginIT extends SolrTestCaseJ4 {

  private static Jedis jedis;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());

    try {
      jedis = new Jedis("localhost");
      jedis.flushAll();
      jedis.sadd("test_key", "test");
    } catch (Exception ex) {}
  }

  @Test
  public void shouldFindSingleDocument() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());{
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis method=smembers key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKey() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis method=smembers key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocuments() {
    String[] doc1 = {"id", "1", "string_field", "test"};
    String[] doc2 = {"id", "2", "string_field", "other_key"};
    String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_key", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis method=smembers key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    try {
      jedis.quit();
    } catch (Exception ex) {
    }
  }
}
