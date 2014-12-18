package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

  @Test
  public void testExtractTwoSubqueryFields() {
    Query q1 = new TermQuery(new Term("field1", "value1"));
    Query q2 = new TermQuery(new Term("field2", "value2"));
    
    BooleanQuery booleanQuery = new BooleanQuery();
    BooleanQueryExtractor booleanQueryExtractor = new BooleanQueryExtractor();

    booleanQuery.add(new BooleanClause(q1, BooleanClause.Occur.MUST));
    booleanQuery.add(new BooleanClause(q2, BooleanClause.Occur.MUST));

    Set<String> extractedFieldNames = new HashSet<>();

    booleanQueryExtractor.extractSubQueriesFields(booleanQuery, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(2, extractedFieldNames.size());
    assertTrue(extractedFieldNames.contains("field1"));
    assertTrue(extractedFieldNames.contains("field2"));
  }

}
