package com.sematext.solr.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * ParserPlugin which builds a query parser basing on data stored in Redis.
 * <p> RedisQParserPlugin initiates connection with Redis and pass the connection
 * object to RedisQParser which is responsible for fetching data and building a
 * query.
 *
 * @author prog
 * @author lstrojny
 */
public class RedisQParserPlugin extends QParserPlugin {

  /**
   * Redis parameter name constant.
   */
  public static final String NAME = "redis";

  /**
   * Logger
   */
  private static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  /**
   * Host name parameter name constant
   */
  private static final String HOST_FIELD = "host";

  /**
   * Maximum number of connections to redis parameter name constant
   */
  private static final String MAX_CONNECTIONS_FIELD = "maxConnections";

  /**
   * Number of retries parameter name constant
   */
  private static final String RETRIES_FIELD = "retries";

  /**
   * Database name parameter name constant
   */
  private static final String DATABASE_FIELD = "database";

  /**
   * Password parameter name constant
   */
  private static final String PASSWORD_FIELD = "password";

  /**
   * Redis parameter name constant
   */
  private static final String TIMEOUT_FIELD = "timeout";

  /**
   * Default number of connections limit
   */
  private static final int DEFAULT_MAX_CONNECTIONS = 5;

  /**
   * Default number of operation retries
   */
  private static final int DEFAULT_RETRIES = 1;

  /**
   * Jedis connection handler
   */
  private CommandHandler connectionHandler;

  @Override
  public QParser createParser(final String qstr, final SolrParams localParams, final SolrParams params,
    final SolrQueryRequest req) {
    return new RedisQParser(qstr, localParams, params, req, connectionHandler);
  }

  @Override
  public void init(final NamedList args) {
    final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMaxTotal(getInt(args, MAX_CONNECTIONS_FIELD, DEFAULT_MAX_CONNECTIONS));

    final String host = getString(args, HOST_FIELD, HostAndPort.LOCALHOST_STR);
    final int timeout = getInt(args, TIMEOUT_FIELD, Protocol.DEFAULT_TIMEOUT);
    final String password = getString(args, PASSWORD_FIELD, null);
    final int database = getInt(args, DATABASE_FIELD, Protocol.DEFAULT_DATABASE);
    final int retries = getInt(args, RETRIES_FIELD, DEFAULT_RETRIES);

    final String[] hostAndPort = host.split(":");
    final JedisPool jedisConnectionPool = createPool(poolConfig, hostAndPort[0],
        hostAndPort.length == 2 ? Integer.parseInt(hostAndPort[1]) : Protocol.DEFAULT_PORT, timeout, password,
        database);

    connectionHandler = createCommandHandler(jedisConnectionPool, retries);

    log.info("Initialized RedisQParserPlugin with host: " + host);
  }

  /**
   * Creates redis connection pool.
   *
   * @param poolConfig Pool configuration
   * @param host Hostname
   * @param port Port
   * @param timeout Timeout value optional
   * @param password Password optional
   * @param database Database name optional
   * @return Prepared connection pool
   */
  JedisPool createPool(final GenericObjectPoolConfig poolConfig, final String host, final int port, final int timeout,
    final String password, final int database) {
    return new JedisPool(poolConfig, host, port, timeout, password, database);
  }

  /**
   * Create a new command handler
   *
   * @param connectionPool Redis connection pool
   * @param retries How often should a failed operation be retried
   * @return Relevant command handler
   */
  CommandHandler createCommandHandler(final JedisPool connectionPool, final int retries) {
    return new RetryingCommandHandler(connectionPool, retries);
  }

  /**
   * Extract integer value from parameters list.
   *
   * @param args Arguments list
   * @param key Name of field
   * @param def Default value
   * @return Integer value of parameter with given name
   */
  private int getInt(final NamedList args, final String key, final int def) {
    final Object value = args != null ? args.get(key) : null;
    return value instanceof String ? Integer.parseInt((String) value) : def;
  }

  /**
   * Extract string  value from parameters list.
   *
   * @param args Arguments list
   * @param key Name of field
   * @param def Default value
   * @return String value of parameter with given name
   */
  private String getString(final NamedList args, final String key, final String def) {
    final Object value = args != null ? args.get(key) : null;
    return value instanceof String ? (String) value : def;
  }
}
