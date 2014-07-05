package com.sematext.solr.redis;

import com.google.common.annotations.VisibleForTesting;
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
 */
public class RedisQParserPlugin extends QParserPlugin {

  private static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  public static final String NAME = "redis";
  private static final String HOST_FIELD = "host";
  private static final String MAX_CONNECTIONS_FIELD = "maxConnections";
  private static final String RETRIES_FIELD = "retries";
  private static final String DATABASE_FIELD = "database";
  private static final String PASSWORD_FIELD = "password";
  private static final String TIMEOUT_FIELD = "timeout";

  private static final int DEFAULT_MAX_CONNECTIONS = 5;
  private static final int DEFAULT_RETRIES = 1;

  private JedisPool jedisConnectorPool;

  private int retries = DEFAULT_RETRIES;

  @Override
  public QParser createParser(final String qstr, final SolrParams localParams, final SolrParams params,
    final SolrQueryRequest req) {
    return new RedisQParser(qstr, localParams, params, req, jedisConnectorPool, retries);
  }

  @Override
  public void init(final NamedList args) {
    final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMaxTotal(getInteger(args, MAX_CONNECTIONS_FIELD, DEFAULT_MAX_CONNECTIONS));

    final String host = getString(args, HOST_FIELD, HostAndPort.LOCALHOST_STR);
    final Integer timeout = getInteger(args, TIMEOUT_FIELD, Protocol.DEFAULT_TIMEOUT);
    final String password = getString(args, PASSWORD_FIELD, null);
    final Integer database = getInteger(args, DATABASE_FIELD, Protocol.DEFAULT_DATABASE);
    retries = getInteger(args, RETRIES_FIELD, DEFAULT_RETRIES);

    log.info("Initializing RedisQParserPlugin with host: " + host);
    final String[] hostAndPort = host.split(":");
    jedisConnectorPool = createPool(poolConfig, hostAndPort[0],
        hostAndPort.length == 2 ? Integer.parseInt(hostAndPort[1]) : Protocol.DEFAULT_PORT, timeout, password,
        database);
  }

  @VisibleForTesting
  public int getRetries() {
    return retries;
  }

  JedisPool createPool(final GenericObjectPoolConfig poolConfig, final String host, final int port, final int timeout,
    final String password, final int database) {
    return new JedisPool(poolConfig, host, port, timeout, password, database);
  }

  private Integer getInteger(final NamedList args, final String key, final Integer def) {
    final Object value = args != null ? args.get(key) : null;
    return value instanceof String ? Integer.parseInt((String) value) : def;
  }

  private String getString(final NamedList args, final String key, final String def) {
    final Object value = args != null ? args.get(key) : null;
    return value instanceof String ? (String) value : def;
  }
}
