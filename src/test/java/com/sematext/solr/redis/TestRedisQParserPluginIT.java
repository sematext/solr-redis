package com.sematext.solr.redis;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.search.BooleanQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.net.URLDecoder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRedisQParserPluginIT extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(TestRedisQParserPluginIT.class);
  
  private Jedis jedis;
  
  private static RedisServer redisServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    redisServer = RedisServer.builder()
        .port(6379)
        .setting("bind localhost")
        .build();
    redisServer.start();
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
    } catch (final RuntimeException ex) {
      log.error("Error when configuring local Jedis connection", ex);
    }
  }

  @Test
  public void shouldRespectBoosting() {
    final String[] doc1 = {"id", "1", "string_field", "test"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    final String[] doc3 = {"id", "3", "string_field", "another_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "string_field:another_key^100  {!redis command=smembers key=test_set v=string_field}");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");

    params = new ModifiableSolrParams();
    params.add("q", "string_field:another_key^100  {!redis command=smembers key=test_set v=string_field}^101");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentSmembers() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySmembers() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSmembers() {
    final String[] doc1 = {"id", "1", "string_field", "test"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    final String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentSrandmember() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySrandmember() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSrandmember() {
    final String[] doc1 = {"id", "1", "string_field", "test"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    final String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set", "other_key");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=srandmember key=test_set count=2}string_field");
    assertQ(req(params), "*[count(//doc)=3]");
  }

  @Test
  public void shouldFindSingleDocumentSunion() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeySunion() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsSunion() {
    final String[] doc1 = {"id", "1", "string_field", "test"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    final String[] doc3 = {"id", "3", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_set2", "other_key");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sunion key=test_set key1=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentHget() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHget() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentHvals() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHvals() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsHvals() {
    final String[] doc1 = {"id", "1", "string_field", "value1"};
    final String[] doc2 = {"id", "2", "string_field", "value2"};
    final String[] doc3 = {"id", "3", "string_field", "value3"};
    final String[] doc4 = {"id", "4", "string_field", "value4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hvals key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=3]");
  }

  @Test
  public void shouldFindThreeDocumentsHmget() {
    final String[] doc1 = {"id", "1", "string_field", "value0"};
    final String[] doc2 = {"id", "2", "string_field", "value1"};
    final String[] doc3 = {"id", "3", "string_field", "value2"};
    final String[] doc4 = {"id", "4", "string_field", "value3"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field0=key0 field1=key1 fieldfoo=key2 fieldbla= key2=key3}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentHmget() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHmget() {
    final String[] doc = {"id", "1", "string_field", "value1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hmget key=test_hash field=key1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentHkeys() {
    final String[] doc = {"id", "1", "string_field", "key1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyHkeys() {
    final String[] doc = {"id", "1", "string_field", "key1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsHkeys() {
    final String[] doc1 = {"id", "1", "string_field", "key1"};
    final String[] doc2 = {"id", "2", "string_field", "key2"};
    final String[] doc3 = {"id", "3", "string_field", "key3"};
    final String[] doc4 = {"id", "4", "string_field", "key4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.hset("test_hash", "key0", "value0"); // Ignored
    jedis.hset("test_hash", "key2", "value2");
    jedis.hset("test_hash", "key3", "value3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=hkeys key=test_hash}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentGet() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.set("test_key", "test");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyGet() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldInflateGzipFindSingleDocumentGet() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.set("test_key".getBytes(), Compressor.compressGzip("test".getBytes()));

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key compression=gzip}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldDeserializeJsonFindMultipleDocumentsGet() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "test2"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    jedis.set("test_key", "['test1','test2']");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key serialization=json}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldInflateGzipAndDeserializeJsonFindMultipleDocumentsGet() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "test2"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    jedis.set("test_key".getBytes(), Compressor.compressGzip("[\"test1\",\"test2\"]".getBytes()));

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key serialization=json compression=gzip}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldIgnoreIllegalJsonGet() {
    jedis.set("test_key".getBytes(), Compressor.compressGzip("[".getBytes()));

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key serialization=json}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldIgnoreIllegalCompressiomGet() {
    jedis.set("test_key".getBytes(), "0".getBytes());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=get key=test_key serialization=json}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsMget() {
    final String[] doc1 = {"id", "1", "string_field", "one"};
    final String[] doc2 = {"id", "2", "string_field", "two"};
    final String[] doc3 = {"id", "3", "string_field", "three"};
    final String[] doc4 = {"id", "4", "string_field", "four"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.set("test_key1", "one");
    jedis.set("test_key2", "two");
    jedis.set("test key3", "three");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=test_key1 key0=test_key2 keyfoo='test key3' keybla= key2=emptyKey}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindSingleDocumentMget() {
    final String[] doc = {"id", "1", "string_field", "one"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.set("test_key1", "one");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=test_key1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyMget() {
    final String[] doc = {"id", "1", "string_field", "one"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=mget key=key}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentLrange() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLrange() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindThreeDocumentsLrange() {
    final String[] doc1 = {"id", "1", "string_field", "element1"};
    final String[] doc2 = {"id", "2", "string_field", "element2"};
    final String[] doc3 = {"id", "3", "string_field", "element3"};
    final String[] doc4 = {"id", "4", "string_field", "element4"}; // Ignored
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.lpush("test_list", "element0"); // Ignored
    jedis.lpush("test_list", "element2");
    jedis.lpush("test_list", "element3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lrange key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldOrderDocumentsLrange() {
    final String[] doc1 = {"id", "1", "string_field", "element1"};
    final String[] doc2 = {"id", "2", "string_field", "element2"};
    final String[] doc3 = {"id", "3", "string_field", "element3"};
    final String[] doc4 = {"id", "4", "string_field", "element4"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.rpush("test_list2", "element3");
    jedis.rpush("test_list2", "element4");
    jedis.rpush("test_list2", "element1");
    jedis.rpush("test_list2", "element2");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=lrange key=test_list2}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='4']");
  }

  @Test
  public void shouldFindSingleDocumentLindex() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindSingleDocumentLindexWithExplicitIndex() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=0}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindSingleDocumentLindexWithExplicitNegativeIndex() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=-1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLindex() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindNoDocumentOnMissingRedisKeyLindexWithExplicitIndex() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=lindex key=test_list index=1}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindTwoDocumentsOnZrevrangebyscore() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=zrevrangebyscore key=test_set min=2 max=3}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldOrderDocumentsByScoreZrevrangebyscore() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.zadd("test_set2", 3, "member3");
    jedis.zadd("test_set2", 2, "member1");
    jedis.zadd("test_set2", 1, "member2");
    jedis.zadd("test_set2", 0, "member4");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrevrangebyscore key=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindSingleDocumentOnZrevrangebyscoreWithInfSymbol() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    final String[] doc4 = {"id", "4", "string_field", "member4"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(adoc(doc4));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=zrevrangebyscore key=test_set min='-inf' max=1}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindTwoDocumentsOnZrevrange() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrevrange key=test_set range_start=1 range_end=2 boost=10}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldOrderDocumentsByScoreZrevrange() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.zadd("test_set2", 3, "member3");
    jedis.zadd("test_set2", 2, "member1");
    jedis.zadd("test_set2", 1, "member2");
    jedis.zadd("test_set2", 0, "member4");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrevrange key=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindTwoDocumentsOnZrange() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.flushAll();

    jedis.zadd("test_set", 1, "member1");
    jedis.zadd("test_set", 2, "member2");
    jedis.zadd("test_set", 3, "member3");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrange key=test_set range_start=1 range_end=2}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[2]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldOrderDocumentsByScoreZrange() {
    final String[] doc1 = {"id", "1", "string_field", "member1"};
    final String[] doc2 = {"id", "2", "string_field", "member2"};
    final String[] doc3 = {"id", "3", "string_field", "member3"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.zadd("test_set2", 3, "member3");
    jedis.zadd("test_set2", 2, "member1");
    jedis.zadd("test_set2", 1, "member2");
    jedis.zadd("test_set2", 0, "member4");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=zrange key=test_set2}string_field");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='1']", "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindNoDocumentOnMissingSortKey() {
    final String[] doc = {"id", "1", "string_field", "element1"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.flushAll();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void shouldFindSingleDocumentOnSortKey() {
    final String[] doc = {"id", "1", "string_field", "100"};
    assertU(adoc(doc));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKey() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "20"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=sort key=test_sort_list}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsOrder() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "20"};
    final String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsOrderDesc() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "20"};
    final String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list order=desc}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsConfiguredAlphaOrder() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "20"};
    final String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndKeepsConfiguredAlphaOrderDesc() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "20"};
    final String[] doc3 = {"id", "3", "string_field", "10"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("test_sort_list", "100");
    jedis.sadd("test_sort_list", "20");
    jedis.sadd("test_sort_list", "10");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha order=desc}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndOrderBySecondaryKeys() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "200"};
    final String[] doc3 = {"id", "3", "string_field", "300"};
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

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list by=weight_*}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndOrderBySecondaryKeysWithAlgorithmAlpha() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "200"};
    final String[] doc3 = {"id", "3", "string_field", "300"};
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

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list algorithm=alpha by=weight_*}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']");
  }

  @Test
  public void shouldFindMultipleDocumentOnSortKeyAndFetchSecondaryObject() {
    final String[] doc1 = {"id", "1", "string_field", "100"};
    final String[] doc2 = {"id", "2", "string_field", "200"};
    final String[] doc3 = {"id", "3", "string_field", "300"};
    final String[] doc4 = {"id", "4", "string_field", "400"};
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

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=sort key=test_sort_list by=sort_* get0=obj0_* get1=obj1_*}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']", "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='4']");
  }

  @Test
  public void shouldRetryOnConnectionProblem() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!redis command=smembers key=test_set}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    jedis.configSet("timeout", "1");
    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldReturnSingleDocumentOnEvalWithString() {
    final String[] doc = {"id", "1", "string_field", "test"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");

    params.set("fq", "{!redis command=eval script='return KEYS[1];' key=test}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    params.set("fq", "{!redis command=eval script='return KEYS[1];' key=test}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return ARGV[1];' arg=test}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    params.set("fq", "{!redis command=eval script='return ARGV[1];' arg=test}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return \\'test\\';'}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    params.set("fq", "{!redis command=eval script='return \\'test\\';'}string_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldReturnSingleDocumentOnEvalWithInteger() {
    String hash;
    final String[] doc1 = {"id", "1", "int_field", "100"};
    final String[] doc2 = {"id", "2", "int_field", "200"};
    final String[] doc3 = {"id", "2", "int_field", "300"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");

    params.set("fq", "{!redis command=eval script='return KEYS[1] + 0;' key=100 useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return KEYS[1] + 0;");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " key=100 useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return ARGV[1] + 0;' arg=100 useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return ARGV[1] + 0;");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " arg=100 useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return 100;' useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return 100;");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " useAnalyzer=true}int_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldReturnSingleDocumentOnEvalWithDouble() {
    String hash;
    final String[] doc = {"id", "1", "double_field", "1.123"};
    assertU(adoc(doc));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");

    params.set("fq", "{!redis command=eval script='return KEYS[1];' key=1.123 useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return KEYS[1];");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " key=1.123 useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return ARGV[1];' arg=1.123 useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return ARGV[1];");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " arg=1.123 useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    params.set("fq", "{!redis command=eval script='return \\'1.123\\';' useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
    hash = sha1("return '1.123';");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " useAnalyzer=true}double_field");
    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldReturnSingleDocumentOnEvalWithList() {
    String hash;
    final String[] doc1 = {"id", "1", "string_field", "one"};
    final String[] doc2 = {"id", "2", "string_field", "two"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");

    params.set("fq", "{!redis command=eval script='return KEYS;' key=one key0=two}string_field");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
    hash = sha1("return KEYS;");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " key=one key0=two}string_field");
    params.set("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");

    params.set("fq", "{!redis command=eval script='return ARGV;' arg=one arg0=two}string_field");
    params.set("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
    hash = sha1("return ARGV;");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + " arg=one arg0=two}string_field");
    params.set("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");

    params.set("fq", "{!redis command=eval script='return {\\'one\\', \\'two\\'};'}string_field");
    params.set("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
    hash = sha1("return {'one', 'two'};");
    params.set("fq", "{!redis command=evalsha sha1=" + hash + "}string_field");
    params.set("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void shouldReturnSingleDocumentOnEvalWithHash() {
    final String[] doc1 = {"id", "1", "string_field", "one"};
    final String[] doc2 = {"id", "2", "string_field", "two"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return {\\'one\\', \\'1.2\\', \\'two\\', \\'1.3\\'};' returns_hash=true}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
    final String hash = sha1("return {'one', '1.2', 'two', '1.3'};");
    params = new ModifiableSolrParams();
    params.set("q", "{!redis command=evalsha sha1=" + hash + " returns_hash=true}string_field");
    assertQ(req(params), "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionOnEvalWithExpectedHashAndUnevenNumberOfElements() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return {1, 2, 3};' returns_hash=true}string_field");
    JQ(req(params));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionOnEvalStringAsKey() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return {\\'foo\\', \\'bar\\'};' returns_hash=true}string_field");
    JQ(req(params));
  }

  @Test
  public void shouldHandleNullFromEval() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return nil;'}string_field");
    JQ(req(params));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionOnEvalReturningNestedTable() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return {1, {1}};'}string_field");
    JQ(req(params));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionOnEvalReturningNestedTableAsHash() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=eval script='return {1, {1}};' returns_hash=true}string_field");
    JQ(req(params));
  }

  @Test
  public void shouldScoreDocumentsWithMultivaluedFields() throws Exception {
    final String[] doc1 = {"id", "1", "interestIds", "1", "interestIds", "2"};
    final String[] doc2 = {"id", "2", "interestIds", "2", "interestIds", "3", "interestIds", "4"};
    final String[] doc3 = {"id", "3", "interestIds", "3", "interestIds", "4", "interestIds", "5"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("interests", "2");
    jedis.sadd("interests", "3");
    jedis.sadd("interests", "4");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=smembers key=interests}interestIds");
    params.set("fl", "score,interestIds,id");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']", "//result/doc[3]/str[@name='id'][.='1']");
  }

  @Test
  public void shouldIgnoreScoreDocuments() throws Exception {
    final String[] doc1 = {"id", "1", "interestIds", "1", "interestIds", "2"};
    final String[] doc2 = {"id", "2", "interestIds", "2", "interestIds", "3", "interestIds", "4"};
    final String[] doc3 = {"id", "3", "interestIds", "3", "interestIds", "4", "interestIds", "5"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(adoc(doc3));
    assertU(commit());

    jedis.sadd("interests", "2");
    jedis.sadd("interests", "3");
    jedis.sadd("interests", "4");

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "{!redis command=smembers key=interests ignoreScore=true}interestIds");
    params.set("fl", "score,interestIds,id");
    assertQ(req(params), "*[count(//doc)=3]", "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/float[@name='score'][.=1.0]", "//result/doc[3]/float[@name='score'][.=1.0]");
  }

  //We use TermsQuery now
  public void shouldHandleMoreThanMaxBooleanClausesLimit() throws Exception {
    final int size = 1025;
    final String[] values = new String[size];
    for (Integer a = 0; a < size; a++) {
      values[a] = a.toString();
    }

    jedis.sadd("overlong_set", values);

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=smembers key=overlong_set}string_field");

    try {
      JQ(req(params));
    } catch (final BooleanQuery.TooManyClauses ignored) {
    }

    // Run again to make sure we are in the right state
    JQ(req(params));
  }

  private static String sha1(final String input) {
    return DigestUtils.sha1Hex(input);
  }

  /**
   * Utility to print the result of a query
   */
  private static void debugQueryParams(final SolrParams params) {
    try {
      final SolrQueryRequest req = req(params);
      System.out.println("REQUEST: " + URLDecoder.decode(req.getParamString(), "UTF-8"));
      System.out.println("RESPONSE: " + JQ(req));
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    try {
      jedis.quit();
    } catch (final RuntimeException ignored) {
    }
  }
  
  @AfterClass
  public static void afterClass() { 
    redisServer.stop();
  } 
}

