package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestConstantScoreQueryExtractor extends TestQueryExtractor {

  @Test
  public void testExtractWrappedQuery() {
    Query q1 = mock(Query.class);

    ConstantScoreQueryExtractor constantScoreQueryExtractor = new ConstantScoreQueryExtractor();
    ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(q1);

    List<Query> extractedQueries = new ArrayList<>();

    constantScoreQueryExtractor.extract(constantScoreQuery, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(1, extractedQueries.size());
    assertEquals(q1, extractedQueries.get(0));
  }

  @Test
  public void testDoNotExtractWrappedFilterAndReturnConstantScoreQuery() {
    Filter f1 = mock(Filter.class);

    ConstantScoreQueryExtractor constantScoreQueryExtractor = new ConstantScoreQueryExtractor();
    ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(f1);

    List<Query> extractedQueries = new ArrayList<>();

    constantScoreQueryExtractor.extract(constantScoreQuery, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(1, extractedQueries.size());
    assertTrue(extractedQueries.get(0) instanceof ConstantScoreQuery);
    assertEquals(constantScoreQuery, extractedQueries.get(0));
  }
}
