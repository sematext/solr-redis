package com.sematext.lucene.query.extractor;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

public abstract class QueryExtractor<T extends Query> {

  private static final ImmutableList<QueryExtractor<? extends Query>> DEFAULT_EXTRACTORS = ImmutableList.of(
      new BooleanQueryExtractor(),
      new DisjunctionQueryExtracotr(),
      new ConstantScoreQueryExtractor(),
      new FilteredQueryExtractor(),
      new PhraseQueryExtractor(),
      new TermQueryExtractor(),
      new TaggedQueryExtractor(),
      new GenericQueryExtractor()
  );

  private final Class<T> cls;

  protected QueryExtractor(Class<T> cls) {
    this.cls = cls;
  }

  public abstract void extract(T q, Iterable<QueryExtractor<? extends Query>> extractors, List<Query> extractedQueries)
          throws UnsupportedOperationException;

  public abstract void extractSubQueriesFields(T q, Iterable<QueryExtractor<? extends Query>> extractors, Set<String> extractedFields)
          throws UnsupportedOperationException;

  public static void extractQuery(Query q, List<Query> extractedQueries) throws UnsupportedOperationException {
    extractQuery(q, DEFAULT_EXTRACTORS, extractedQueries);
  }

  public static void extractQuery(Query q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    for (QueryExtractor extractor : extractors) {
      if (extractor.cls.isAssignableFrom(q.getClass())) {
        extractor.extract(q, extractors, extractedQueries);
        return;
      }
    }
    throw new UnsupportedOperationException("No extractor found for class: " + q.getClass());
  }

  public static void extractFields(Query q, Set<String> extractedFields) throws UnsupportedOperationException {
    extractFields(q, DEFAULT_EXTRACTORS, extractedFields);
  }

  public static void extractFields(Query q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
    for (QueryExtractor extractor : extractors) {
      if (extractor.cls.isAssignableFrom(q.getClass())) {
        extractor.extractSubQueriesFields(q, extractors, extractedFields);
        return;
      }
    }
    throw new UnsupportedOperationException("No extractor found for class: " + q.getClass());
  }

}
