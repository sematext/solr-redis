package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ScriptingCommands;
import java.util.Map;

public class Eval extends ScriptingCommand {
  private static final Logger log = LoggerFactory.getLogger(Eval.class);

  @Override
  protected Map<String, Float> invokeCommand(final ScriptingCommands client, final SolrParams params,
    final int keyLength, final String[] args) {
    final String script = ParamUtil.assertGetStringByName(params, "script");

    log.debug("Fetching EVAL from Redis for script: {}", script);

    return createReturnValue(params, client.eval(script, keyLength, args));
  }
}
