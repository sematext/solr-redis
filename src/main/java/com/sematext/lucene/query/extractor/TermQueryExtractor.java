package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * TermQueryExtractor. Extractor for TermQuery.
 * This is lower (leaf) level extractor.
 *
 * @author prog
 */
public class TermQueryExtractor extends QueryExtractor<TermQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public TermQueryExtractor() {
    super(TermQuery.class);
  }

  @Override
  public void extract(final TermQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(final TermQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final Set<String> extractedFields) throws UnsupportedOperationException {
    extractedFields.add(q.getTerm().field());
  }

}
