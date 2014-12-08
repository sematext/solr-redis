package com.sematext.solr.highlighter;

import com.sematext.lucene.query.TaggedQuery;
import com.sematext.lucene.query.extractor.QueryExtractor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaggedQueryHighlighter extends DefaultSolrHighlighter {

  private static Logger log = LoggerFactory.getLogger(TaggedQueryHighlighter.class);

  public TaggedQueryHighlighter() {
  }

  public TaggedQueryHighlighter(SolrCore solrCore) {
    super(solrCore);
  }

  @Override
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields)
          throws IOException {

    List<TaggedQuery> taggedQueries = new ArrayList<>();
    try {
      List<Query> extractedQueries = new ArrayList<>();
      QueryExtractor.extractQuery(query, extractedQueries);
      for (Query extractedQuery : extractedQueries) {
        if (extractedQuery instanceof TaggedQuery) {
          taggedQueries.add((TaggedQuery) extractedQuery);
        }
      }
    } catch (UnsupportedOperationException ex) {
      log.warn("Cannot highlight query.", ex);
    }

    if (taggedQueries.isEmpty()) {
      return super.doHighlighting(docs, query, req, defaultFields);
    } else {
      for (TaggedQuery taggedQuery : taggedQueries) {
        NamedList<Object> partialHighlight = super.doHighlighting(docs, taggedQuery, req, defaultFields);
        //System.out.println(partialHighlight);
        return super.doHighlighting(docs, taggedQuery.getWrappedQuery(), req, defaultFields);
      }
      return new SimpleOrderedMap();
    }
  }

}
