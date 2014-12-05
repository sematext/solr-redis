package com.sematext.lucene.query.extractor;

import java.util.List;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

public class FilteredQueryExtractor extends QueryExtractor<FilteredQuery> {

  public FilteredQueryExtractor() {
    super(FilteredQuery.class);
  }

  @Override
  public void extract(FilteredQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractQuery(q.getQuery(), extractors, extractedQueries);
    } else {
      extractQuery(q, extractors, extractedQueries);
    }
  }

}
