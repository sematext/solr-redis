package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;

/**
 * Exctractor for ConstantScoreQuery.
 * It extracts inner query. If there is no inner query and only filter this extractor will do nothing.
 *
 * @author prog
 */
public class ConstantScoreQueryExtractor extends QueryExtractor<ConstantScoreQuery> {

  /**
   * Default constructor.
   * It only uses super class constructor giving as an argument query class.
   */
  public ConstantScoreQueryExtractor() {
    super(ConstantScoreQuery.class);
  }

  @Override
  public void extract(final ConstantScoreQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractQuery(q.getQuery(), extractors, extractedQueries);
    } else {
      extractedQueries.add(q);
    }
  }

  @Override
  public void extractSubQueriesFields(final ConstantScoreQuery q,
          final Iterable<QueryExtractor<? extends Query>> extractors,
          final Set<String> extractedFields) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractFields(q.getQuery(), extractors, extractedFields);
    }
  }

}
