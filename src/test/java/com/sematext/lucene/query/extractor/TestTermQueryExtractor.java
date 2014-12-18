package com.sematext.lucene.query.extractor;

import static com.sematext.lucene.query.extractor.TestQueryExtractor.DEFAULT_EXTRACTORS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    @Test
  public void testExtractEmptyFieldSet() {
    TermQuery q = new TermQuery(new Term("field", "value"));

    TermQueryExtractor termQueryExtractor = new TermQueryExtractor();
    Set<String> extractedFieldNames = new HashSet<>();

    termQueryExtractor.extractSubQueriesFields(q, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(1, extractedFieldNames.size());
    assertTrue(extractedFieldNames.contains("field"));
  }
}
