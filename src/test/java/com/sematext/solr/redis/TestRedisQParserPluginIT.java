package com.sematext.solr.redis;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
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
      jedis.sadd("test_set", "test");
      jedis.hset("test_hash", "key1", "value1");
      jedis.lpush("test_list", "element1");

    } catch (Exception ignored) {}
  }

  @Test
  public void shouldRespectBoosting() {
    String[] doc1 = {"id", "1", "string_field", "test"};
    String[] doc2 = {"id", "2", "string_field", "other_key"};
    String[] doc3 = {"id", "3", "string_field", "another_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "string_field:another_key^100  {!redis command=smembers key=test_set v=string_field}");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");

    params = new ModifiableSolrParams();
    params.add("q", "string_field:another_key^100  {!redis command=smembers key=test_set v=string_field}^101");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentSmembers() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySmembers() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSmembers() {
    String[] doc1 = {"id", "1", "string_field", "test"};
    String[] doc2 = {"id", "2", "string_field", "other_key"};
    String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentSrandmember() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySrandmember() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSrandmember() {
    String[] doc1 = {"id", "1", "string_field", "test"};
    String[] doc2 = {"id", "2", "string_field", "other_key"};
    String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set count=2}string_field");
    assertQ(req(params), "*[count(//doc)=3]");
  }

  @Test
  public void shouldFindSingleDocumentSunion() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySunion() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSunion() {
    String[] doc1 = {"id", "1", "string_field", "test"};
    String[] doc2 = {"id", "2", "string_field", "other_key"};
    String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set2", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set key1=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentHget() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHget() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentHvals() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHvals() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsHvals() {
    String[] doc1 = {"id", "1", "string_field", "value1"};
    String[] doc2 = {"id", "2", "string_field", "value2"};
    String[] doc3 = {"id", "3", "string_field", "value3"};
    String[] doc4 = {"id", "4", "string_field", "value4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=3]");
  }

  @Test
  public void shouldFindThreeDocumentsHmget() {
    String[] doc1 = {"id", "1", "string_field", "value0"};
    String[] doc2 = {"id", "2", "string_field", "value1"};
    String[] doc3 = {"id", "3", "string_field", "value2"};
    String[] doc4 = {"id", "4", "string_field", "value3"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field0=key0 field1=key1 fieldfoo=key2 fieldbla= key2=key3}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentHmget() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHmget() {
    String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentHkeys() {
    String[] doc = {"id", "1", "string_field", "key1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHkeys() {
    String[] doc = {"id", "1", "string_field", "key1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsHkeys() {
    String[] doc1 = {"id", "1", "string_field", "key1"};
    String[] doc2 = {"id", "2", "string_field", "key2"};
    String[] doc3 = {"id", "3", "string_field", "key3"};
    String[] doc4 = {"id", "4", "string_field", "key4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentGet() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.set("test_key", "test");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyGet() {
    String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsMget() {
    String[] doc1 = {"id", "1", "string_field", "one"};
    String[] doc2 = {"id", "2", "string_field", "two"};
    String[] doc3 = {"id", "3", "string_field", "three"};
    String[] doc4 = {"id", "4", "string_field", "four"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.set("test_key1", "one");
    jedis.set("test_key2", "two");
    jedis.set("test key3", "three");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=test_key1 key0=test_key2 keyfoo='test key3' keybla= key2=emptyKey}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentMget() {
    String[] doc = {"id", "1", "string_field", "one"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.set("test_key1", "one");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=test_key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyMget() {
    String[] doc = {"id", "1", "string_field", "one"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=key}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentLrange() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLrange() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsLrange() {
    String[] doc1 = {"id", "1", "string_field", "element1"};
    String[] doc2 = {"id", "2", "string_field", "element2"};
    String[] doc3 = {"id", "3", "string_field", "element3"};
    String[] doc4 = {"id", "4", "string_field", "element4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.lpush("test_list", "element0"); // Ignored
    jedis.lpush("test_list", "element2");
    jedis.lpush("test_list", "element3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldOrderDocumentsLrange() {
    String[] doc1 = {"id", "1", "string_field", "element1"};
    String[] doc2 = {"id", "2", "string_field", "element2"};
    String[] doc3 = {"id", "3", "string_field", "element3"};
    String[] doc4 = {"id", "4", "string_field", "element4"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.rpush("test_list2", "element3");
    jedis.rpush("test_list2", "element4");
    jedis.rpush("test_list2", "element1");
    jedis.rpush("test_list2", "element2");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=lrange key=test_list2}string_field");
    assertQ(req(params), "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='4']", "//result/doc[3]/str[@name='id'][.='1']",
        "//result/doc[4]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindSingleDocumentLindex() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindSingleDocumentLindexWithExplicitIndex() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=0}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindSingleDocumentLindexWithExplicitNegativeIndex() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=-1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLindex() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLindexWithExplicitIndex() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindTwoDocumentsOnZrevrangebyscore() {
    String[] doc1 = {"id", "1", "string_field", "member1"};
    String[] doc2 = {"id", "2", "string_field", "member2"};
    String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=zrevrangebyscore key=test_set min=2 max=3}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldOrderDocumentsByScoreZrevrange() {
    String[] doc1 = {"id", "1", "string_field", "member1"};
    String[] doc2 = {"id", "2", "string_field", "member2"};
    String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.zadd("test_set2", 3, "member3");
    jedis.zadd("test_set2", 2, "member1");
    jedis.zadd("test_set2", 1, "member2");
    jedis.zadd("test_set2", 0, "member4");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrevrangebyscore key=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindSingleDocumentOnZrevrangebyscoreWithInfSymbol() {
    String[] doc1 = {"id", "1", "string_field", "member1"};
    String[] doc2 = {"id", "2", "string_field", "member2"};
    String[] doc3 = {"id", "3", "string_field", "member3"};
    String[] doc4 = {"id", "4", "string_field", "member4"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=zrevrangebyscore key=test_set min='-inf' max=1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingSortKey() {
    String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentOnSortKey() {
    String[] doc = {"id", "1", "string_field", "100"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKey() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "20"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsOrder() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "20"};
    String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsOrderDesc() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "20"};
    String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list order=desc}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsConfiguredAlphaOrder() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "20"};
    String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsConfiguredAlphaOrderDesc() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "20"};
    String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha order=desc}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndOrderBySecondaryKeys() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "200"};
    String[] doc3 = {"id", "3", "string_field", "300"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "200");
    jedis.sadd("test_sort_list", "300");
    jedis.set("weight_100", "10");
    jedis.set("weight_200", "1");
    jedis.set("weight_300", "2");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list by=weight_*}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']", "//result/doc[3]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndOrderBySecondaryKeysWithAlgorithmAlpha() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "200"};
    String[] doc3 = {"id", "3", "string_field", "300"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "200");
    jedis.sadd("test_sort_list", "300");
    jedis.set("weight_100", "1");
    jedis.set("weight_200", "2");
    jedis.set("weight_100", "10");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha by=weight_*}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndFetchSecondaryObject() {
    String[] doc1 = {"id", "1", "string_field", "100"};
    String[] doc2 = {"id", "2", "string_field", "200"};
    String[] doc3 = {"id", "3", "string_field", "300"};
    String[] doc4 = {"id", "4", "string_field", "400"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.sadd("test_sort_list", "1");
    jedis.sadd("test_sort_list", "2");
    jedis.set("obj0_1", "300");
    jedis.set("obj0_2", "400");
    jedis.set("obj1_1", "100");
    jedis.set("obj1_2", "200");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list by=sort_* get0=obj0_* get1=obj1_*}string_field");
    assertQ(req(params), "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='2']");
  }

  /**
   * Utility to print the result of a query
   */
  private static void debugQuery(SolrParams params) {
    try {
      final SolrQueryRequest req = req(params);
      System.out.println("REQUEST: " + java.net.URLDecoder.decode(req.getParamString(), "UTF-8"));
      System.out.println("RESPONSE: " + JQ(req));
    } catch (Exception e) {
      e.printStackTrace();
    }
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
