package com.sematext.solr.redis;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestRedisQParserPlugin {

  private RedisQParserPlugin parserPlugin;

  @Before
  public void setUp() {
    initMocks(this);
    parserPlugin = new RedisQParserPlugin();
  }

  @Test
  public void shouldReturnInstanceOfQParserPlugin() {
    NamedList<String> localParams = new NamedList<>();
    localParams.add("method", "smembers");
    localParams.add("key", "key");

    QParser createParser = parserPlugin.createParser("test", SolrParams.toSolrParams(localParams),
            SolrParams.toSolrParams(new NamedList()), null);

    assertNotNull(createParser);
  }
}
