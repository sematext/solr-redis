package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

public class GenericQueryExtractor extends QueryExtractor<Query> {

  public GenericQueryExtractor() {
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
