package com.sematext.lucene.query.extractor;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

public class TestQueryExtractor {

  public static final ImmutableList<QueryExtractor<? extends Query>> DEFAULT_EXTRACTORS = ImmutableList.of(
      new TermQueryExtractor(),
      new FakeExtractor()
  );

  private static class FakeExtractor extends QueryExtractor<Query> {

    public FakeExtractor() {
      super(Query.class);
    }

    @Override
    public void extract(Query q, Iterable<QueryExtractor<? extends Query>> extractors, List<Query> extractedQueries) {
      extractedQueries.add(q);
    }

    @Override
    public void extractSubQueriesFields(Query q, Iterable<QueryExtractor<? extends Query>> extractors,
            Set<String> extractedFields) throws UnsupportedOperationException {
    }

  }

}
