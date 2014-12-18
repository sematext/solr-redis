package com.sematext.lucene.query.extractor;

import static com.sematext.lucene.query.extractor.TestQueryExtractor.DEFAULT_EXTRACTORS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestFilteredQueryExtractor {

  @Test
  public void testExtractWrappedQuery() {
    Query q1 = mock(Query.class);
    Filter f1 = mock(Filter.class);

    FilteredQueryExtractor filteredQueryExtractor = new FilteredQueryExtractor();
    FilteredQuery filteredQuery = new FilteredQuery(q1, f1);

    List<Query> extractedQueries = new ArrayList<>();

    filteredQueryExtractor.extract(filteredQuery, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(1, extractedQueries.size());
    assertEquals(q1, extractedQueries.get(0));
  }

  @Test
  public void testExtractSubqueryField() {
    Query q1 = new TermQuery(new Term("field1", "value1"));
    Filter f1 = mock(Filter.class);

    FilteredQueryExtractor filteredQueryExtractor = new FilteredQueryExtractor();
    FilteredQuery filteredQuery = new FilteredQuery(q1, f1);

    Set<String> extractedFieldNames = new HashSet<>();

    filteredQueryExtractor.extractSubQueriesFields(filteredQuery, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(1, extractedFieldNames.size());
    assertTrue(extractedFieldNames.contains("field1"));
  }
}
