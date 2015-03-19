package com.sematext.solr.redis;

import com.sematext.solr.redis.command.Command;
import org.apache.solr.common.params.SolrParams;
import java.util.Map;

/**
 * Handles Redis command execution
 *
 * @author lstrojny
 */
interface CommandHandler {
  /**
   * Executes a Redis command
   *
   * @param command The command to execute
   * @param localParams Solr Local params of the tag
   * @return Map of value (string) and score (float). Score may be NaN
   */
  Map<String, Float> executeCommand(final Command command, final SolrParams localParams);
}
