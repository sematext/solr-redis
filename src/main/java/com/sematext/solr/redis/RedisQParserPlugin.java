package com.sematext.solr.redis;

import java.util.HashSet;
import java.util.Set;
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
 * Create a boolean query with multiple tokens.
 */
public class RedisQParserPlugin extends QParserPlugin {
  public static final String NAME = "redis";
  public static final String HOST_FIELD = "host";
  
  static final Logger log = LoggerFactory.getLogger(RedisQParserPlugin.class);

  private Set<HostAndPort> hosts = new HashSet<>();
  
  private JedisPool jedisConnectorPool;
  private JedisCommands jedis;


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
        log.info("Initialization of RedisQParserPlugin failed. No redis host configuration");
      }
    }
  }
}
