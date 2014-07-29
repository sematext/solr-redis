package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import java.util.ArrayList;
import java.util.Iterator;

final class ParamUtil {
  private ParamUtil() {
    super();
  }

  static int assertGetIntByName(final SolrParams params, final String param) {
    final int value = tryGetIntByName(params, param, null);

    throwIfNull(param, value);

    return value;
  }

  static Integer tryGetIntByName(final SolrParams params, final String param, final Integer defaultValue) {
    final String value = params.get(param);
    if (isEmpty(value)) {
      return defaultValue;
    }

    try {
      return Integer.parseInt(value);
    } catch (final NumberFormatException ex) {
      return defaultValue;
    }
  }

  static String assertGetStringByName(final SolrParams params, final String param) {
    final String value = tryGetStringByName(params, param, null);

    throwIfNull(param, value);

    return value;
  }

  static String tryGetStringByName(final SolrParams params, final String param, final String defaultValue) {
    final String value = params.get(param);
    if (isEmpty(value)) {
      return defaultValue;
    }

    return value;
  }

  static boolean assertGetBooleanByName(final SolrParams params, final String param) {
    final boolean value = tryGetBooleanByName(params, param, null);

    throwIfNull(param, value);

    return value;
  }

  static Boolean tryGetBooleanByName(final SolrParams params, final String param, final Boolean defaultValue) {
    final String value = params.get(param);
    if (isEmpty(value)) {
      return defaultValue;
    }

    return Boolean.parseBoolean(value);
  }

  static String[] getStringByPrefix(final SolrParams params, final CharSequence prefix) {
    final Iterator<String> it = params.getParameterNamesIterator();
    final ArrayList<String> keyList = new ArrayList<>();

    while (it != null && it.hasNext()) {
      final String paramKey = it.next();
      if (paramKey.length() < prefix.length() || !paramKey.substring(0, prefix.length()).equals(prefix)) {
        continue;
      }

      final String newKey = tryGetStringByName(params, paramKey, "");
      if (!"".equals(newKey)) {
        keyList.add(newKey);
      }
    }

    return keyList.toArray(new String[keyList.size()]);
  }

  private static boolean isEmpty(final String value) {
    return value == null || "".equals(value);
  }

  private static void throwIfNull(final String param, final Object defaultValue) {
    if (defaultValue == null) {
      throw new IllegalArgumentException(String.format("Required parameter \"%s\" missing", param));
    }
  }
}
