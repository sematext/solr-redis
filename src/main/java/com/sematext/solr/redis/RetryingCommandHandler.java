package com.sematext.solr.redis;

import com.sematext.solr.redis.command.Command;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import java.util.Map;

/**
 * @author lstrojny
 */
class RetryingCommandHandler implements CommandHandler {
  /**
   * Logger
   */
  private static final Logger log = LoggerFactory.getLogger(RetryingCommandHandler.class);

  /**
   * Redis connection pool
   */
  private final JedisPool connectionPool;

  /**
   * Maximum number of retries
   */
  private final int maxRetries;

  /**
   * Constructor
   *
   * @param connectionPool Redis connection pool
   * @param maxRetries Maximum number of retries
   */
  public RetryingCommandHandler(final JedisPool connectionPool, final int maxRetries) {
    this.connectionPool = connectionPool;
    this.maxRetries = maxRetries;
  }

  @Override
  public Map<String, Float> executeCommand(final Command command, final SolrParams localParams) {
    int retries = 0;

    Map<String, Float> results = null;
    final long methodInvocationStart = currentTimeMillis();

    do {
      Jedis jedis = null;
      final long commandInvocationStart = currentTimeMillis();

      try {
        jedis = connectionPool.getResource();
        results = command.execute(jedis, localParams);
        connectionPool.returnResource(jedis);

        break;

      } catch (final JedisException e) {
        connectionPool.returnBrokenResource(jedis);

        log.warn("Redis communication error occurred with Jedis${} after {}ms for command {} on {}: {}. Retry {} of {}",
            System.identityHashCode(jedis), currentTimeMillis() - commandInvocationStart, command.getClass().getName(),
            localParams.get("key"), e.getMessage(), retries + 1, maxRetries);

        if (retries >= maxRetries) {
          throw e;
        }

      }
    } while (retries++ < maxRetries);

    if (retries > 0) {
      log.warn("Redis communication error recovered after {}ms and {} of {} retries for command {} on {}",
          currentTimeMillis() - methodInvocationStart, retries, maxRetries, command.getClass().getName(),
          localParams.get("key"));
    }

    return results;
  }

    /** @return Current time in milliseconds */
  private static long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
