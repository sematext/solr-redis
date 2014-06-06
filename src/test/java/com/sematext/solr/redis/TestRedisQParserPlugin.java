package com.sematext.solr.redis;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestRedisQParserPlugin {

  private RedisQParserPlugin parserPlugin;

  @Mock
  private JedisPool jedisPool;

  @Before
  public void setUp() {
    initMocks(this);
    parserPlugin = new RedisQParserPlugin();
    parserPlugin.setJedisConnectorPool(jedisPool);
  }

  @Test
  public void shouldReturnInstanceOfQParserPlugin() {
    NamedList<String> localParams = new NamedList<>();
    localParams.add("method", "smembers");
    localParams.add("key", "key");

    when(jedisPool.getResource()).thenReturn(mock(Jedis.class));

    QParser createParser = parserPlugin.createParser("test", SolrParams.toSolrParams(localParams),
            SolrParams.toSolrParams(new NamedList()), null);

    assertNotNull(createParser);
  }
}
