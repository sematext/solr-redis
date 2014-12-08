package com.sematext.solr.highlighter;

import com.sematext.lucene.query.TaggedQuery;
import com.sematext.lucene.query.extractor.QueryExtractor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private static final String MAIN_HIGHLIGHT = "##default##";

  public TaggedQueryHighlighter() {
  }

  public TaggedQueryHighlighter(SolrCore solrCore) {
    super(solrCore);
  }

  @Override
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields)
          throws IOException {

    List<TaggedQuery> taggedQueries = new ArrayList<>();
    List<Query> otherQueries = new ArrayList<>();
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
      log.debug("Running default highlighter. No tagged queries are used in main query.");
      return super.doHighlighting(docs, query, req, defaultFields);
    } else {
      log.debug("Collecting highlights for Running default highlighter. No tagged queries are used in main query.");
      Map<String, SimpleOrderedMap> results = new HashMap<>();
      results.put(MAIN_HIGHLIGHT, (SimpleOrderedMap) super.doHighlighting(docs, query, req, defaultFields));
      for (TaggedQuery taggedQuery : taggedQueries) {
        SimpleOrderedMap partialResult =
                (SimpleOrderedMap) super.doHighlighting(docs, taggedQuery.getWrappedQuery(), req, defaultFields);
        results.put(taggedQuery.getTag(), partialResult);
      }

      return mergeResults(results);
    }
  }


  private SimpleOrderedMap mergeResults(Map<String, SimpleOrderedMap> results) {
    SimpleOrderedMap mergedResult = new SimpleOrderedMap();
    for (Map.Entry<String, SimpleOrderedMap> partialResultEntry : results.entrySet()) {
      for (Object subResultEntryObject : partialResultEntry.getValue()) {
        Map.Entry<String, Object> subResultEntry = (Map.Entry<String, Object>) subResultEntryObject;
        for (Object docEntryObject : (NamedList) subResultEntry.getValue()) {
          Map.Entry<String, Object> docEntry = (Map.Entry<String, Object>) docEntryObject;
          String fieldName = partialResultEntry.getKey();
          //If results are from main highlight we should add original field name. In other case we should use
          //field alias which comes from tagged query
          if (MAIN_HIGHLIGHT.equals(fieldName)) {
            fieldName = docEntry.getKey();
          }
          addFragmentToDoc(mergedResult, subResultEntry.getKey(), fieldName, (String[]) docEntry.getValue());
        }
      }
    }
    return mergedResult;
  }

  private void addFragmentToDoc(SimpleOrderedMap result, String docId, String fieldName, String[] fragments) {
    Object subResultObject = result.get(docId);
    if (result.get(docId) == null) {
      subResultObject = new SimpleOrderedMap();
      result.add(docId, subResultObject);
    }
    SimpleOrderedMap subResult = (SimpleOrderedMap) subResultObject;
    List<String> fieldResult = null;
    if (subResult.get(fieldName) == null) {
      fieldResult = new ArrayList<>();
      subResult.add(fieldName, fieldResult);
    }
    fieldResult = (List<String>) subResult.get(fieldName);
    fieldResult.addAll(Arrays.asList(fragments));
  }

}
