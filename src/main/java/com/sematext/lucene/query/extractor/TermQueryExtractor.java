package com.sematext.lucene.query.extractor;

import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TermQueryExtractor extends QueryExtractor<TermQuery> {

  public TermQueryExtractor() {
    super(TermQuery.class);
  }

  @Override
  public void extract(TermQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    extractedQueries.add(q);
  }

}
