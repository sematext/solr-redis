package com.sematext.lucene.query.extractor;

import static com.sematext.lucene.query.extractor.TestQueryExtractor.DEFAULT_EXTRACTORS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestGenericQueryExtractor extends TestQueryExtractor {

  @Test
  public void testReturnTheSameQuery() {
    Query q = mock(Query.class);

    GenericQueryExtractor genericQueryExtractor = new GenericQueryExtractor();

    List<Query> extractedQueries = new ArrayList<>();

    genericQueryExtractor.extract(q, DEFAULT_EXTRACTORS, extractedQueries);
    assertEquals(1, extractedQueries.size());
    assertEquals(q, extractedQueries.get(0));
  }

  @Test
  public void testExtractEmptyFieldSet() {
    Query q = mock(Query.class);

    GenericQueryExtractor genericQueryExtractor = new GenericQueryExtractor();
    Set<String> extractedFieldNames = new HashSet<>();

    genericQueryExtractor.extractSubQueriesFields(q, DEFAULT_EXTRACTORS, extractedFieldNames);
    assertEquals(0, extractedFieldNames.size());
  }
}
