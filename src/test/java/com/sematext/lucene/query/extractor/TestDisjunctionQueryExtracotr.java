package com.sematext.lucene.query.extractor;

import static com.sematext.lucene.query.extractor.TestQueryExtractor.DEFAULT_EXTRACTORS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

  @Test
  public void testExtractSubqueryField() {
    Query q1 = new TermQuery(new Term("field1", "value1"));
    Query q2 = new TermQuery(new Term("field2", "value2"));

    DisjunctionQueryExtracotr disjunctionQueryExtracotr = new DisjunctionQueryExtracotr();

    DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(0.0f);
    disjunctionMaxQuery.add(q1);
    disjunctionMaxQuery.add(q2);

    Set<String> extractedFieldNames = new HashSet<>();

    disjunctionQueryExtracotr.extractSubQueriesFields(disjunctionMaxQuery, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(2, extractedFieldNames.size());
    assertTrue(extractedFieldNames.contains("field1"));
    assertTrue(extractedFieldNames.contains("field2"));
  }
}
