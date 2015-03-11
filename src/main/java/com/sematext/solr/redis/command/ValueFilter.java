package com.sematext.solr.redis.command;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class ValueFilter {
  private static final Logger log = LoggerFactory.getLogger(ValueFilter.class);

  Map<String, Float> filterValue(final SolrParams params, final byte[] byteValue)
      throws UnsupportedAlgorithmException, DeserializationException {
    final String compression = ParamUtil.tryGetStringByName(params, "compression", "");
    final String serializationForm = ParamUtil.tryGetStringByName(params, "serialization", "");

    return deserialize(serializationForm, inflate(compression, byteValue));
  }

  private static String inflate(final String compression, final byte[] bytes) throws UnsupportedAlgorithmException {
    if ("".equals(compression)) {
      return new String(bytes);
    } else if ("gzip".equals(compression)) {
      return inflateGzip(bytes);
    } else {
      throw new UnsupportedAlgorithmException(String.format("Unsupported algorithm: '%s'", compression));
    }
  }

  private static Map<String, Float> deserialize(final String serializationFormat, final String value)
      throws DeserializationException {

    if ("".equals(serializationFormat)) {
      return ResultUtil.stringIteratorToMap(Collections.singletonList(value));
    } else if ("json".equals(serializationFormat)) {
      return ResultUtil.stringIteratorToMap(Arrays.asList(deserializeJson(value)));
    } else {
      throw new DeserializationException(String.format("Unsupported serialization format: '%s'", serializationFormat));
    }
  }

  private static String inflateGzip(final byte[] bytes) {
    log.debug("Decompressing GZIP data");

    try {
      try (
          final GZIPInputStream stream = new GZIPInputStream(new ByteArrayInputStream(bytes));
          final BufferedReader buffer = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      ) {
        String value = "";
        String line;
        while ((line = buffer.readLine()) != null) {
          value += line;
        }
        return value;
      }
    } catch (final UnsupportedEncodingException e) {
      log.warn("Unsupported encoding, using string as is: {}", e.getMessage());
      return new String(bytes);
    } catch (final IOException e) {
      log.warn("Compression exception, using string as is: {}", e.getMessage());
      return new String(bytes);
    }
  }

  private static String[] deserializeJson(final String value) {
    log.debug("Deserialization JSON data");

    try {
      return new Gson().fromJson(value, String[].class);
    } catch (final JsonSyntaxException e) {
      log.warn("Deserialization error, using string as is: {}", e.getMessage());
      return new String[]{value};
    }
  }
}
