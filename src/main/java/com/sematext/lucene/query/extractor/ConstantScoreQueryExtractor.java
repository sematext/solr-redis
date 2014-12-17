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
  public void extract(ConstantScoreQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractQuery(q.getQuery(), extractors, extractedQueries);
    }
    else
    {
      extractQuery(q, extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(ConstantScoreQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractFields(q.getQuery(), extractors, extractedFields);
    }
  }

}
