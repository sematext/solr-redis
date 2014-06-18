package com.sematext.solr.redis;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import static org.junit.Assert.assertNotNull;
import org.apache.solr.search.QParserPlugin;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestRedisQParserPlugin {

  private QParserPlugin parserPlugin;

  @Before
  public void setUp() {
    initMocks(this);
    parserPlugin = new RedisQParserPlugin();
  }

  @Test
  public void shouldReturnInstanceOfQParserPlugin() {
    NamedList<String> localParams = new NamedList<>();
    localParams.add("command", "smembers");
    localParams.add("key", "key");

    QParser createParser = parserPlugin.createParser("test", SolrParams.toSolrParams(localParams),
            SolrParams.toSolrParams(new NamedList()), null);

    assertNotNull(createParser);
  }
}
