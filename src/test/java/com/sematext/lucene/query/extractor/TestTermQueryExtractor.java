package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestTermQueryExtractor extends TestQueryExtractor {

  @Test
  public void testReturnTheSameQuery() {
    TermQuery q = new TermQuery(new Term("field", "value"));

    TermQueryExtractor termQueryExtractor = new TermQueryExtractor();

    List<Query> extractedQueries = new ArrayList<>();

    termQueryExtractor.extract(q, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(1, extractedQueries.size());
    assertEquals(q, extractedQueries.get(0));
  }
}
