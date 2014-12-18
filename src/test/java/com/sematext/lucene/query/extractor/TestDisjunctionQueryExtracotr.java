package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestDisjunctionQueryExtracotr extends TestQueryExtractor {

  @Test
  public void testExtractTwoSubqueries() {
    Query q1 = mock(Query.class);
    Query q2 = mock(Query.class);

    DisjunctionQueryExtractor disjunctionQueryExtracotr = new DisjunctionQueryExtractor();

    DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(0.0f);
    disjunctionMaxQuery.add(q1);
    disjunctionMaxQuery.add(q2);

    List<Query> extractedQueries = new ArrayList<>();

    disjunctionQueryExtracotr.extract(disjunctionMaxQuery, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(2, extractedQueries.size());
    assertEquals(q1, extractedQueries.get(0));
    assertEquals(q2, extractedQueries.get(1));
  }
}
