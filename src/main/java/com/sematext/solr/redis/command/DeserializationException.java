package com.sematext.solr.redis.command;

public class DeserializationException extends Exception {
  private static final long serialVersionUID = -5231842833274667633L;

  public DeserializationException(final String message) {
    super(message);
  }

  public DeserializationException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public DeserializationException(final Throwable cause) {
    super(cause);
  }

  protected DeserializationException(final String message, final Throwable cause,
                                     final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
