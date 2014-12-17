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
  public void extract(Query q, Iterable<QueryExtractor<? extends Query>> extractors, List<Query> extractedQueries) {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(Query q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
  }

}
