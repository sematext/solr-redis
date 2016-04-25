package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Exctractor for TermsQuery. It extracts all inner queries (from all clauses)
 *
 * @author smock
 */
public class TermsQueryExtractor extends QueryExtractor<TermsQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public TermsQueryExtractor() {
    super(TermsQuery.class);
  }

  @Override
  public void extract(final TermsQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    final TermIterator iterator = q.getTermData().iterator();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      extractedQueries.add(new TermQuery(new Term(iterator.field(), term)));
    }
  }

  @Override
  public void extractSubQueriesFields(final TermsQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
      final Set<String> extractedFields) throws UnsupportedOperationException {
    final TermIterator iterator = q.getTermData().iterator();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      extractedFields.add(iterator.field());
    }
  }

}
