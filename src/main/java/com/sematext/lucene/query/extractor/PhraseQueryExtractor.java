package com.sematext.lucene.query.extractor;

import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

public class PhraseQueryExtractor extends QueryExtractor<PhraseQuery> {

  public PhraseQueryExtractor() {
    super(PhraseQuery.class);
  }

  @Override
  public void extract(PhraseQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(PhraseQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
    for (Term term : q.getTerms()) {
      extractedFields.add(term.field());
    }
  }

}
