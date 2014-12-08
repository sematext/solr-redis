package com.sematext.lucene.query.extractor;

import static com.sematext.lucene.query.extractor.TestQueryExtractor.DEFAULT_EXTRACTORS;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
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
}
