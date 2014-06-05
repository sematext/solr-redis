package com.sematext.solr.redis;

import org.junit.Before;

public class TestRedisQParserPlugin {
  RedisQParserPlugin parserPlugin;

  @Before
  public void setUp() {
    parserPlugin = new RedisQParserPlugin();
  }
}
