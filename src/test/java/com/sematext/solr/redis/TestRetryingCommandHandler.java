package com.sematext.solr.redis;

import com.sematext.solr.redis.command.Command;

import org.junit.After;
import org.junit.Assert;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

public class TestRetryingCommandHandler {
  @Mock
  private JedisPool jedisPool;

  @Mock
  private Jedis jedisOne;

  @Mock
  private Jedis jedisTwo;

  @Mock
  private Jedis jedisThree;

  @Mock
  private Command command;

  @Mock
  private SolrParams localParams;

  private final Map<String, Float> expectedResult = new HashMap<>();
  
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

  @Test
  public void shouldRetryExactlyOnceAndSucceedOnTheFirstRetry() {
    when(jedisPool.getResource())
        .thenReturn(jedisOne)
        .thenReturn(jedisTwo);
    when(command.execute(jedisOne, localParams)).thenThrow(new JedisConnectionException("Connection exception"));
    when(command.execute(jedisTwo, localParams)).thenReturn(expectedResult);

    final Map<String, Float> result = createCommandHandler(1).executeCommand(command, localParams);
    Assert.assertEquals(expectedResult, result);

    verify(jedisPool, times(1)).returnBrokenResource(jedisOne);
    verify(jedisPool, times(1)).returnResource(jedisTwo);
  }

  @Test(expected = JedisException.class)
  public void shouldRetryExactlyOnceAndGiveUpWhenFirstRetryFails() {
    when(jedisPool.getResource())
        .thenReturn(jedisOne)
        .thenReturn(jedisTwo);
    when(command.execute(jedisOne, localParams))
        .thenThrow(new JedisConnectionException("Connection exception"));
    when(command.execute(jedisTwo, localParams))
        .thenThrow(new JedisConnectionException("Connection exception"));

    try {
      createCommandHandler(1).executeCommand(command, localParams);
      throw new RuntimeException("Should fail");
    } catch (final JedisException e) {
      verify(jedisPool, times(1)).returnBrokenResource(jedisOne);
      verify(jedisPool, times(1)).returnBrokenResource(jedisTwo);

      throw e;
    }
  }

  @Test(expected = JedisException.class)
  public void shouldNeverRetry() {
    when(jedisPool.getResource())
        .thenReturn(jedisOne);
    when(command.execute(jedisOne, localParams))
        .thenThrow(new JedisConnectionException("Connection exception"));

    try {
      createCommandHandler(0).executeCommand(command, localParams);
      throw new RuntimeException("Should fail");
    } catch (final JedisException e) {
      verify(jedisPool, times(1)).returnBrokenResource(jedisOne);

      throw e;
    }
  }

  @Test(expected = JedisException.class)
  public void shouldNeverRetryWhenExceptionHappensWhileGettingResource() {
    when(jedisPool.getResource())
        .thenThrow(new JedisConnectionException("Pool exception"));

    try {
      createCommandHandler(0).executeCommand(command, localParams);
      throw new RuntimeException("Should fail");
    } catch (final JedisException e) {
      verify(jedisPool, times(1)).returnBrokenResource(null);

      throw e;
    }
  }

  @Test
  public void shouldRetryExactlyTwiceAndSucceedOnTheSecondRetry() {
    when(jedisPool.getResource())
        .thenReturn(jedisOne)
        .thenReturn(jedisTwo)
        .thenReturn(jedisThree);
    when(command.execute(jedisOne, localParams)).thenThrow(new JedisConnectionException("Connection exception"));
    when(command.execute(jedisTwo, localParams)).thenThrow(new JedisConnectionException("Connection exception"));
    when(command.execute(jedisThree, localParams)).thenReturn(expectedResult);

    final Map<String, Float> result = createCommandHandler(2).executeCommand(command, localParams);
    Assert.assertEquals(expectedResult, result);

    verify(jedisPool, times(1)).returnBrokenResource(jedisOne);
    verify(jedisPool, times(1)).returnBrokenResource(jedisTwo);
    verify(jedisPool, times(1)).returnResource(jedisThree);
  }

  private CommandHandler createCommandHandler(final int maxRetries) {
    return new RetryingCommandHandler(jedisPool, maxRetries);
  }
}
