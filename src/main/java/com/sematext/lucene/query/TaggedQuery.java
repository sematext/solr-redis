package com.sematext.lucene.query;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 * This query is used to tag original query.
 *
 * Query wraps original one and adds metadata to it.
 */
public class TaggedQuery extends Query {

  private final Query wrappedQuery;
  private final String tag;

  public TaggedQuery(Query q, String tag) {
    this.wrappedQuery = q;
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public Query getWrappedQuery() {
    return wrappedQuery;
  }
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return wrappedQuery.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append("TaggedQuery [");
    builder.append(wrappedQuery.toString());
    builder.append("]");
    return builder.toString();
  }

}
