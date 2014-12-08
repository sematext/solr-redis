package com.sematext.lucene.query.extractor;

import java.util.ArrayList;
import java.util.List;
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
}
