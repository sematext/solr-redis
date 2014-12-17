package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

/**
 * Exctractor for FilteredQuery.
 * It extracts inner query.
 *
 * @author prog
 */
public class FilteredQueryExtractor extends QueryExtractor<FilteredQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public FilteredQueryExtractor() {
    super(FilteredQuery.class);
  }

  @Override
  public void extract(final FilteredQuery q,
          final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractQuery(q.getQuery(), extractors, extractedQueries);
    }
    else
    {
      extractQuery(q, extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(final FilteredQuery q,
          final Iterable<QueryExtractor<? extends Query>> extractors,
          final Set<String> extractedFields) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractFields(q.getQuery(), extractors, extractedFields);
    }
  }

}
