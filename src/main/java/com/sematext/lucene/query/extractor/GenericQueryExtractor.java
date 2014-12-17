package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

/**
 * GenericQueryExtractor. Used if no other extractors matches. Lower level (leaf) extractor.
 * @author prog
 */
public class GenericQueryExtractor extends QueryExtractor<Query> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public GenericQueryExtractor() {
    super(Query.class);
  }

  @Override
  public void extract(final Query q,
          final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(final Query q,
          final Iterable<QueryExtractor<? extends Query>> extractors,
          final Set<String> extractedFields) throws UnsupportedOperationException {
  }

}
