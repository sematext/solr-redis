package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;

public class DisjunctionQueryExtracotr extends QueryExtractor<DisjunctionMaxQuery> {

  public DisjunctionQueryExtracotr() {
    super(DisjunctionMaxQuery.class);
  }

  @Override
  public void extract(DisjunctionMaxQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    for (Query internalQuery : q) {
      extractQuery(internalQuery, extractors, extractedQueries);
    }
  }

  @Override
  public void extractSubQueriesFields(DisjunctionMaxQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
    for (Query internalQuery : q) {
      extractFields(internalQuery, extractors, extractedFields);
    }
  }

}
