package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestBooleanQueryExtractor extends TestQueryExtractor {

  @Test
  public void testExtractTwoSubqueries() {
    Query q1 = mock(Query.class);
    Query q2 = mock(Query.class);
    
    BooleanQuery booleanQuery = new BooleanQuery();
    BooleanQueryExtractor booleanQueryExtractor = new BooleanQueryExtractor();

    booleanQuery.add(new BooleanClause(q1, BooleanClause.Occur.MUST));
    booleanQuery.add(new BooleanClause(q2, BooleanClause.Occur.MUST));

    List<Query> extractedQueries = new ArrayList<>();

    booleanQueryExtractor.extract(booleanQuery, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(2, extractedQueries.size());
    assertEquals(q1, extractedQueries.get(0));
    assertEquals(q2, extractedQueries.get(1));
  }

}
