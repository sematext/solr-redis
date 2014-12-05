package com.sematext.lucene.query;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestTaggedQuery {

  @Test
  public void testRewrite() throws IOException {
    MemoryIndex memoryIndex = new MemoryIndex();

    TaggedQuery taggedQuery = new TaggedQuery(new TermQuery(new Term("field", "value")), "tag");
    Query rewrittenQuery = taggedQuery.rewrite(memoryIndex.createSearcher().getTopReaderContext().reader());

    assertTrue(rewrittenQuery instanceof TermQuery);
    assertEquals("field", ((TermQuery) rewrittenQuery).getTerm().field());
    assertEquals("value", ((TermQuery) rewrittenQuery).getTerm().text());
  }

  @Test
  public void testSaveTag() throws IOException {
    TaggedQuery taggedQuery = new TaggedQuery(new TermQuery(new Term("field", "value")), "tag");
    assertEquals("tag", taggedQuery.getTag());
  }
}
