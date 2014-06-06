package com.sematext.solr.redis;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

/**
 * ParserPlugin which builds a query parser basing on data stored in Redis.
 * <p> RedisQParserPlugin initiates connection with Redis and pass the connection
 * object to RedisQParser which is responsible for fetching data and building a
 * query.
 * <p>
 * Allowed parameters for the RedisQParserPlugin are:
 * <ul>
 * <li><b>method</b> - Method for Redis. (required)</li>
 * <li><b>key</b> - Key used to fetch data from Redis. (required)</li>
 * <li><b>operator</b> - Operator which connects terms taken from Redis. Allowed values are "AND" and "OR".
 * Default operator is OR. (optional)</li>
 * </ul>
 * <p>Example of usage <code>{!redis method=smembers key=some_key}field</code>
 * <br><p>
 * You should configure that query parser plugin in solrconfig.xml first
 * <br><code>
 *   &lt;queryParser name="redis" class="com.sematext.solr.redis.RedisQParserPlugin"&gt;<br>
 *     &lt;str name="host"&gt;localhost&lt;/str&gt;<br>
 *   &lt;/queryParser&gt;<br>
 * </code>
 */
public class RedisQParserPlugin extends QParserPlugin {
  public static final String NAME = "redis";
  public static final String HOST_FIELD = "host";
  
  static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  private JedisPool jedisConnectorPool;


  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    Jedis jedisConnector = null;
    try {
      jedisConnector = jedisConnectorPool.getResource();
      return new RedisQParser(qstr, localParams, params, req, jedisConnector);
    } finally {
      if (jedisConnector != null) {
        jedisConnectorPool.returnResource(jedisConnector);
      }
    }
  }

  @VisibleForTesting
  public void setJedisConnectorPool(JedisPool jedisConnectorPool) {
    this.jedisConnectorPool = jedisConnectorPool;
  }

  @Override
  public void init(NamedList args) {
    if (args != null) {
      SolrParams queryParserConfigurationParameters = SolrParams.toSolrParams(args);
      Object hostConfiguration = args.get(HOST_FIELD);
      if (hostConfiguration instanceof String) {
        String host = (String) hostConfiguration;
        log.info("Initializing RedisQParserPlugin with host: " + host);
        String[] hostAndPort = host.split(":");
        if (hostAndPort.length == 2) {
          jedisConnectorPool = new JedisPool(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        } else {
          jedisConnectorPool = new JedisPool(host);
        }
      } else {
        log.error("Initialization of RedisQParserPlugin failed. No redis host configuration."
                + "Using default host: localhost");
        jedisConnectorPool = new JedisPool(HostAndPort.LOCALHOST_STR);
      }
    } else {
      log.error("Initialization of RedisQParserPlugin failed. No redis configuration."
              + "Using default host: localhost");
      jedisConnectorPool = new JedisPool(HostAndPort.LOCALHOST_STR);
    }
  }
}
