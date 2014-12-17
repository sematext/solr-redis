package com.sematext.lucene.query.extractor;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

/**
 * This abstract class is template for classes which extends org.apache.lucene.search.Query.
 *
 * @author prog
 * @param <T>
 */
public abstract class QueryExtractor<T extends Query> {

  /**
   * This collection is a list of default extractors used to extract field names and queries.
   */
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

  /**
   * Class of extractor
   */
  private final Class<T> cls;

  /**
   * QueryExtractor
   * @param cls Class of extending extractor
   */
  protected QueryExtractor(Class<T> cls) {
    this.cls = cls;
  }

  /**
   * Abstract mehod which should overriden in all subclasses. This method is used for extracting inner queries.
   *
   * @param query Query to extract
   * @param extractors Extractors
   * @param extractedQueries Output parameter. List of extracted queries.
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public abstract void extract(final T query, Iterable<QueryExtractor<? extends Query>> extractors,
      final List<Query> extractedQueries) throws UnsupportedOperationException;

  /**
   *  Abstract mehod which should overriden in all subclasses. This method is used for extracting inner queries fields.
   *
   * @param query Query to extract
   * @param extractors Extractors
   * @param extractedFields Output parameter. Set of extracted field names.
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public abstract void extractSubQueriesFields(final T query,
      final Iterable<QueryExtractor<? extends Query>> extractors,
      final Set<String> extractedFields) throws UnsupportedOperationException;

  /**
   * This static method is used for extracting inner queries using default collection of extractors.
   *
   * @param query Query to extract
   * @param extractedQueries Output parameter. List of extracted queries.
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public static void extractQuery(final Query query, final List<Query> extractedQueries)
      throws UnsupportedOperationException {
    extractQuery(query, DEFAULT_EXTRACTORS, extractedQueries);
  }

  /**
   * Method used for extracting inner queries using given collection of extractors.
   *
   * @param query Query to extract
   * @param extractors Extractors
   * @param extractedQueries Output parameter. List of extracted queries.
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public static void extractQuery(final Query query, final Iterable<QueryExtractor<? extends Query>> extractors,
      final List<Query> extractedQueries) throws UnsupportedOperationException {
    for (QueryExtractor extractor : extractors) {
      if (extractor.cls.isAssignableFrom(query.getClass())) {
        extractor.extract(query, extractors, extractedQueries);
        return;
      }
    }
    throw new UnsupportedOperationException("No extractor found for class: " + query.getClass());
  }

  /**
   * This static method is used for extracting inner queries field names using defult extractros.
   *
   * @param query Query to extract field names from
   * @param extractedFields Output field names
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public static void extractFields(final Query query, final Set<String> extractedFields)
      throws UnsupportedOperationException {
    extractFields(query, DEFAULT_EXTRACTORS, extractedFields);
  }

  /**
   ** This static method is used for extracting inner queries field names using given extractros.
   *
   * @param query Query to extract field names from
   * @param extractors Extractors
   * @param extractedFields Output field names
   *
   * @throws UnsupportedOperationException This method can trhow UnsupportedOperationException
   */
  public static void extractFields(final Query query, final Iterable<QueryExtractor<? extends Query>> extractors,
      final Set<String> extractedFields) throws UnsupportedOperationException {
    for (QueryExtractor extractor : extractors) {
      if (extractor.getCls().isAssignableFrom(query.getClass())) {
        extractor.extractSubQueriesFields(query, extractors, extractedFields);
        return;
      }
    }
    throw new UnsupportedOperationException("No extractor found for class: " + query.getClass());
  }

  public Class<T> getCls() {
    return cls;
  }
}
