package com.sematext.solr.redis.command;

public class UnsupportedAlgorithmException extends Exception {
  private static final long serialVersionUID = -8875629362503003369L;

  public UnsupportedAlgorithmException(final String message) {
    super(message);
  }

  public UnsupportedAlgorithmException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public UnsupportedAlgorithmException(final Throwable cause) {
    super(cause);
  }

  protected UnsupportedAlgorithmException(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
