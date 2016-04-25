package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
  public void testExtractSubqueryField() {
    Query q1 = new TermQuery(new Term("field1", "value1"));

    ConstantScoreQueryExtractor constantScoreQueryExtractor = new ConstantScoreQueryExtractor();
    ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(q1);

    Set<String> extractedFieldNames = new HashSet<>();

    constantScoreQueryExtractor.extractSubQueriesFields(constantScoreQuery, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(1, extractedFieldNames.size());
    assertTrue(extractedFieldNames.contains("field1"));
  }
}
