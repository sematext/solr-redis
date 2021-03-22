package com.sematext.solr.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.*;

public class TestRedisQParserPlugin {
  
  private AutoCloseable mocks;
  
  @Before
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
  }
  
  @After
  public void tearDown() {
    try {
      mocks.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Spy
  private final RedisQParserPlugin parserPlugin = new RedisQParserPlugin();
  private final ArgumentCaptor<GenericObjectPoolConfig> poolConfigArgument = ArgumentCaptor.forClass(GenericObjectPoolConfig.class);
  private final ArgumentCaptor<String> passwordArgument = ArgumentCaptor.forClass(String.class);
  private final ArgumentCaptor<JedisPool> poolArgument = ArgumentCaptor.forClass(JedisPool.class);
  
  @Test
  public void shouldReturnInstanceOfQParserPlugin() {
    final ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.add("command", "smembers");
    localParams.add("key", "key");

    final QParser createParser = parserPlugin.createParser("test", localParams, new ModifiableSolrParams(), null);
    assertNotNull(createParser);
  }

  @Test
  public void shouldConfigurePoolWithDefaultParametersIfNoSpecificConfigurationIsGiven() {
    parserPlugin.init(new NamedList());

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithDefaultParametersIfNullIsGiven() {
    parserPlugin.init(null);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomHost() {
    final NamedList<String> list = new NamedList<>();
    list.add("host", "127.0.0.1");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq("127.0.0.1"),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));
    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomHostAndPort() {
    final NamedList<String> list = new NamedList<>();
    list.add("host", "127.0.0.1:1000");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq("127.0.0.1"),
        eq(1000), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));
    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomPassword() {
    final NamedList<String> list = new NamedList<>();
    list.add("password", "s3cr3t");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), eq("s3cr3t"),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomDatabase() {
    final NamedList<String> list = new NamedList<>();
    list.add("database", "1");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(1));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));

    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomTimeout() {
    final NamedList<String> list = new NamedList<>();
    list.add("timeout", "100");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(100), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));

    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomRetries() {
    final NamedList<String> list = new NamedList<>();
    list.add("retries", "100");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(100));

    assertNull(passwordArgument.getValue());
    assertEquals(5, poolConfigArgument.getValue().getMaxTotal());
  }

  @Test
  public void shouldConfigurePoolWithCustomMaxConnections() {
    final NamedList<String> list = new NamedList<>();
    list.add("maxConnections", "100");
    parserPlugin.init(list);

    verify(parserPlugin).createPool(poolConfigArgument.capture(), eq(HostAndPort.getLocalhost()),
        eq(Protocol.DEFAULT_PORT), eq(Protocol.DEFAULT_TIMEOUT), passwordArgument.capture(),
        eq(Protocol.DEFAULT_DATABASE));
    verify(parserPlugin).createCommandHandler(poolArgument.capture(), eq(1));

    assertNull(passwordArgument.getValue());
    assertEquals(100, poolConfigArgument.getValue().getMaxTotal());
  }
}
