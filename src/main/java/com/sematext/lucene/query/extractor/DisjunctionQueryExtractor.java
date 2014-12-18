package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;

/**
 * Exctractor for ConstantScoreQuery.
 * It extracts all inner/disjuncts queries.
 *
 * @author prog
 */
public class DisjunctionQueryExtractor extends QueryExtractor<DisjunctionMaxQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public DisjunctionQueryExtractor() {
    super(DisjunctionMaxQuery.class);
  }

  @Override
  public void extract(final DisjunctionMaxQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
        final List<Query> extractedQueries) throws UnsupportedOperationException {
    for (Query internalQuery : q) {
      extractQuery(internalQuery, extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(final DisjunctionMaxQuery q,
        final Iterable<QueryExtractor<? extends Query>> extractors,
        final Set<String> extractedFields) throws UnsupportedOperationException {
    for (final Query internalQuery : q) {
      extractFields(internalQuery, extractors, extractedFields);
    }
  }

}
