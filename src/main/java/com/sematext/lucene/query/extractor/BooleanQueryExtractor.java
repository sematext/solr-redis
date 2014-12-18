package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * Exctractor for BooleanQuery. It extracts all inner queries (from all clauses)
 *
 * @author prog
 */
public class BooleanQueryExtractor extends QueryExtractor<BooleanQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public BooleanQueryExtractor() {
    super(BooleanQuery.class);
  }

  @Override
  public void extract(final BooleanQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    final BooleanClause[] clauses = q.getClauses();
    for (final BooleanClause clause : clauses) {
      extractQuery(clause.getQuery(), extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(final BooleanQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
      final Set<String> extractedFields) throws UnsupportedOperationException {
    final BooleanClause[] clauses = q.getClauses();
    for (final BooleanClause clause : clauses) {
      extractFields(clause.getQuery(), extractors, extractedFields);
    }
  }

}
