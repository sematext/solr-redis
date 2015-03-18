package com.sematext.solr.redis.command;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Test;

public class ValueFilterTest {
  private ValueFilter valueFilter;

  @Before
  public void setUp() {
    valueFilter = new ValueFilter();
  }

  @Test(expected = UnsupportedAlgorithmException.class)
  public void shouldThrowExceptionOnUnsupportedCompressionAlgorithm()
      throws DeserializationException, UnsupportedAlgorithmException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("compression", "unsupportedAlgorithm");

    valueFilter.filterValue(params, new byte[]{});
  }

  @Test(expected = DeserializationException.class)
  public void shouldThrowExceptionOnInvalidDeserializationAlgorithm()
      throws DeserializationException, UnsupportedAlgorithmException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("serialization", "unsupported");

    valueFilter.filterValue(params, new byte[]{});
  }
}
