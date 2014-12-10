package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

public class BooleanQueryExtractor extends QueryExtractor<BooleanQuery> {

  public BooleanQueryExtractor() {
    super(BooleanQuery.class);
  }

  @Override
  public void extract(BooleanQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    BooleanClause[] clauses = q.getClauses();
    for (int i = 0; i < clauses.length; ++i) {
      extractQuery(clauses[i].getQuery(), extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(BooleanQuery q, Iterable<QueryExtractor<? extends Query>> extractors, Set<String> extractedFields) throws UnsupportedOperationException {
    BooleanClause[] clauses = q.getClauses();
    for (int i = 0; i < clauses.length; ++i) {
      extractFields(clauses[i].getQuery(), extractors, extractedFields);
    }
  }

}
