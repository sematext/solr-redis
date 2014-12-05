package com.sematext.lucene.query.extractor;

import java.util.List;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;

public class ConstantScoreQueryExtractor extends QueryExtractor<ConstantScoreQuery> {

  public ConstantScoreQueryExtractor() {
    super(ConstantScoreQuery.class);
  }

  @Override
  public void extract(ConstantScoreQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    if (q.getQuery() != null) {
      extractQuery(q.getQuery(), extractors, extractedQueries);
    } else {
      extractQuery(q, extractors, extractedQueries);
    }
  }

}
