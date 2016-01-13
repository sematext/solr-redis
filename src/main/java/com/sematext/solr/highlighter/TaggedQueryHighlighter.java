package com.sematext.solr.highlighter;

import com.sematext.lucene.query.TaggedQuery;
import com.sematext.lucene.query.extractor.QueryExtractor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaggedQueryHighlighter.
 * This Solr highlighter extends org.apache.solr.highlight.DefaultSolrHighlighter to be able to highlight
 * tagged queries. All tagged queries contain tag/label which enables to differentiate all nested tagged
 * queries.
 *
 * @author prog
 */
public class TaggedQueryHighlighter extends DefaultSolrHighlighter {

  /**
   * Logger instance
   */
  private static final Logger logger = LoggerFactory.getLogger(TaggedQueryHighlighter.class);

  /**
   * Mgic  constant which means the default highlight field  (not tagged/virtual fields)
   */
  private static final String MAIN_HIGHLIGHT = "##default##";

  /**
   * TaggedQueryHighlighter which takes solrCore as parameter. It is used to be compatible with
   * org.apache.solr.highlight.DefaultSolrHighlighter.
   *
   * @param solrCore Solr core instance
   */
  public TaggedQueryHighlighter(final SolrCore solrCore) {
    super(solrCore);
  }

  @Override
  public NamedList<Object> doHighlighting(final DocList docs, final Query query,
          final SolrQueryRequest req, final String[] defaultFields)
      throws IOException {

    final Collection<TaggedQuery> taggedQueries = new ArrayList<>();
    final List<Query> otherQueries = new ArrayList<>();
    try {
      final List<Query> extractedQueries = new ArrayList<>();
      QueryExtractor.extractQuery(query, extractedQueries);
      for (final Query extractedQuery : extractedQueries) {
        if (extractedQuery instanceof TaggedQuery) {
          taggedQueries.add((TaggedQuery) extractedQuery);
        }
      }
    } catch (final UnsupportedOperationException ex) {
      logger.warn("Cannot highlight query.", ex);
    }

    if (taggedQueries.isEmpty()) {
      logger.debug("Running default highlighter. No tagged queries are used in main query.");
      return super.doHighlighting(docs, query, req, defaultFields);
    } else {
      logger.debug("Collecting highlights for Running default highlighter. No tagged queries are used in main query.");
      final Map<String, SimpleOrderedMap> results = new HashMap<>();
      results.put(MAIN_HIGHLIGHT, (SimpleOrderedMap) super.doHighlighting(docs, query, req, defaultFields));

      List<String> fieldsNameList = new ArrayList<>();
      if (req.getParams().getParams(HighlightParams.FIELDS).length > 0) {
        fieldsNameList = Arrays.asList(SolrPluginUtils.split(req.getParams().getParams(HighlightParams.FIELDS)[0]));
      }

      final Set<String> originalFields = new HashSet<>(fieldsNameList);
      for (final TaggedQuery taggedQuery : taggedQueries) {
        final Set<String> fields = new HashSet<>();
        QueryExtractor.extractFields(taggedQuery, fields);
        final ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());

        //Continue if original field set doesn't contain subfields or field tag
        if (!containsField(taggedQuery.getTag(), originalFields, fields)) {
          continue;
        }

        params.remove(HighlightParams.FIELDS);
        params.add(HighlightParams.FIELDS, fields.toArray(new String[0]));
        req.setParams(params);
        final SimpleOrderedMap partialResult =
                (SimpleOrderedMap) super.doHighlighting(docs, taggedQuery.getWrappedQuery(), req, defaultFields);
        results.put(taggedQuery.getTag(), partialResult);
      }

      return mergeResults(results);
    }
  }

  /**
   * Checks if field should be highlighted.
   *
   * @param queryTag Field tag
   * @param originalFields All fields passed in hl.fl parameter
   * @param subFields All fields in inner queries of tagged query
   * @return Returns true if field should be highlighted and false otherwise.
   */
  private boolean containsField(final String queryTag, final Set<String> originalFields,
          final Collection<String> subFields) {
    final boolean containsTag = originalFields.contains(queryTag);
    final Collection<String> tmpOriginalField = new HashSet<>(originalFields);
    tmpOriginalField.retainAll(subFields);
    return containsTag || !tmpOriginalField.isEmpty();
  }

  /**
   * Merges all parial results to single response for Solr highlighting
   *
   * @param results Partial results from default highlighting and tagged queries highlighting.
   * @return Returns merged results of default highlighting and tagged queries highlighting.
   */
  private SimpleOrderedMap mergeResults(final Map<String, SimpleOrderedMap> results) {
    final SimpleOrderedMap mergedResult = new SimpleOrderedMap();
    for (final Map.Entry<String, SimpleOrderedMap> partialResultEntry : results.entrySet()) {
      for (final Object subResultEntryObject : partialResultEntry.getValue()) {
        final Map.Entry<String, Object> subResultEntry = (Map.Entry<String, Object>) subResultEntryObject;
        for (final Object docEntryObject : (Iterable<? extends Object>) subResultEntry.getValue()) {
          final Map.Entry<String, Object> docEntry = (Map.Entry<String, Object>) docEntryObject;
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

  /**
   * Adds matching fragment to parial highlighting result.
   *
   * @param result Partial results which should take additional matching fragment.
   * @param docId Document id
   * @param fieldName Field name (real or virtual/alias)
   * @param fragments All fragments to add
   */
  private void addFragmentToDoc(final SimpleOrderedMap result, final String docId,
          final String fieldName, final String[] fragments) {
    Object subResultObject = result.get(docId);
    if (result.get(docId) == null) {
      subResultObject = new SimpleOrderedMap();
      result.add(docId, subResultObject);
    }
    final SimpleOrderedMap subResult = (SimpleOrderedMap) subResultObject;
    List<String> fieldResult;
    if (subResult.get(fieldName) == null) {
      fieldResult = new ArrayList<>();
      subResult.add(fieldName, fieldResult);
    }
    fieldResult = (List<String>) subResult.get(fieldName);
    fieldResult.addAll(Arrays.asList(fragments));
  }

}
