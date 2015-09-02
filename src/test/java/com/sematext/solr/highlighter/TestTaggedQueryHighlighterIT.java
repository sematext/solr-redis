package com.sematext.solr.highlighter;

import org.apache.solr.SolrTestCaseJ4;
import static org.apache.solr.SolrTestCaseJ4.adoc;
import static org.apache.solr.SolrTestCaseJ4.assertU;
import static org.apache.solr.SolrTestCaseJ4.commit;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.highlight.SolrHighlighter;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 *
 * @author prog
 */
public class TestTaggedQueryHighlighterIT extends SolrTestCaseJ4 {

  private Jedis jedis;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("tagged-highlighting-solrconfig.xml", "schema.xml");
    SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());
    assertTrue("wrong highlighter: " + highlighter.getClass(), highlighter instanceof TaggedQueryHighlighter);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());

    try {
      jedis = new Jedis("localhost");
      jedis.flushAll();
      jedis.sadd("test_set1", "test1");
      jedis.sadd("test_set2", "test2");
    } catch (final RuntimeException ignored) {
    }
  }


  @Test
  public void testNoHighlightingWithoutTag() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=smembers key=test_set1 v=string_field}");
    params.add("hl", "true");
    assertQ(req(params),
            "*[count(//doc)=1]",
            "//lst[not(@name='highlighting')]");
  }

  @Test
  public void testNoHighlightingForWrongTag() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=smembers key=test_set1 v=string_field tag=test_tag}");
    params.add("hl", "true");
    params.add("hl.fl", "wrong_tag");
    assertQ(req(params),
            "*[count(//doc)=1]",
            "count(//lst[(@name='highlighting')]/*)=0");
  }

  @Test
  public void testHighlightSingleDocumentByTag() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "other_key"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!redis command=smembers key=test_set1 v=string_field tag=test_tag}");
    params.add("hl", "true");
    params.add("hl.fl", "test_tag");
    assertQ(req(params),
            "*[count(//doc)=1]",
            "count(//lst[@name='highlighting']/*)=1",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='test_tag']/str='<em>test1</em>'");
  }

  @Test
  public void testHighlightByTwoTags() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "test2"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "({!redis command=smembers key=test_set1 v=string_field tag=test_tag1}) "
            + "OR ({!redis command=smembers key=test_set2 v=string_field tag=test_tag2})");
    params.add("hl", "true");
    params.add("hl.fl", "test_tag1,test_tag2");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "count(//lst[@name='highlighting']/*)=2",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='test_tag1']/str='<em>test1</em>'",
            "//lst[@name='highlighting']/lst[@name='2']/arr[@name='test_tag2']/str='<em>test2</em>'");
  }

  @Test
  public void testHighlightWithTagAndSimpleQuery() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "test2"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "({!redis command=smembers key=test_set1 v=string_field tag=test_tag1}) OR (id:2)");
    params.add("hl", "true");
    params.add("hl.fl", "test_tag1");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "count(//lst[@name='highlighting']/*)=1",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='test_tag1']/str='<em>test1</em>'");
  }

  @Test
  public void testHighlightWithTwoTagInNestedQueries() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    final String[] doc2 = {"id", "2", "string_field", "test2"};
    assertU(adoc(doc1));
    assertU(adoc(doc2));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "({!redis command=smembers key=test_set1 v=string_field tag=test_tag1} AND id:1)"
            + " OR ({!redis command=smembers key=test_set2 v=string_field tag=test_tag2} AND id:1)");
    params.add("hl", "true");
    params.add("hl.fl", "test_tag1");
    assertQ(req(params),
            "*[count(//doc)=1]",
            "count(//lst[@name='highlighting']/*)=1",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='test_tag1']/str='<em>test1</em>'");
  }

  @Test
  public void testHighlightNoDuplicates() {
    final String[] doc1 = {"id", "1", "string_field", "test1"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "({!redis command=smembers key=test_set1 v=string_field tag=test_tag1})"
            + " OR ({!redis command=smembers key=test_set1 v=string_field tag=test_tag1})");
    params.add("hl", "true");
    params.add("hl.fl", "test_tag1");
    assertQ(req(params),
            "*[count(//doc)=1]",
            "count(//lst[@name='highlighting']/*)=1",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='test_tag1']/str='<em>test1</em>'");
  }

  @Test
  public void testConstantScoreQueryWithFilterPartOnly() {
    final String[] doc1 = {"id", "1", "location", "56.9485,24.0980"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!geofilt sfield=\"location\" pt=\"56.9484,24.0981\" d=100}");
    params.add("hl", "true");
    params.add("hl.fl", "location");
    assertQ(req(params), "*[count(//doc)=1]", "count(//lst[@name='highlighting']/*)=1");
  }
}
