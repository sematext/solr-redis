package com.sematext.lucene.query.extractor;

import com.sematext.lucene.query.TaggedQuery;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Query;

/**
 * TaggedQueryExtractor. Extractor for TaggedQuery.
 * This is lower (leaf) level extractor and it doesn't try to extract any nested queries from wrapped query.
 *
 * @author prog
 */
public class TaggedQueryExtractor extends QueryExtractor<TaggedQuery> {

  /**
   * Default constructor. It only uses super class constructor giving as an argument query class.
   */
  public TaggedQueryExtractor() {
    super(TaggedQuery.class);
  }

  @Override
  public void extract(final TaggedQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final List<Query> extractedQueries) throws UnsupportedOperationException {
    extractedQueries.add(q);
  }

  @Override
  public void extractSubQueriesFields(final TaggedQuery q, final Iterable<QueryExtractor<? extends Query>> extractors,
          final Set<String> extractedFields) throws UnsupportedOperationException {
    extractFields(q.getWrappedQuery(), extractors, extractedFields);
  }

}
