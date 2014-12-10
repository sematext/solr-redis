package com.sematext.lucene.query.extractor;

import com.sematext.lucene.query.TaggedQuery;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

public class TaggedQueryExtractor extends QueryExtractor<TaggedQuery> {

  public TaggedQueryExtractor() {
    super(TaggedQuery.class);
  }

  @Override
  public void extract(TaggedQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          List<Query> extractedQueries) throws UnsupportedOperationException {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(TaggedQuery q, Iterable<QueryExtractor<? extends Query>> extractors,
          Set<String> extractedFields) throws UnsupportedOperationException {
    extractFields(q.getWrappedQuery(), extractors, extractedFields);
  }

}
