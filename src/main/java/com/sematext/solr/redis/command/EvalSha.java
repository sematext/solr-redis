package com.sematext.solr.redis.command;

import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ScriptingCommands;
import java.util.Map;

public class EvalSha extends ScriptingCommand {
  private static final Logger log = LoggerFactory.getLogger(EvalSha.class);

  @Override
  protected Map<String, Float> invokeCommand(final ScriptingCommands client, final SolrParams params,
    final int keyLength, final String[] args) {
    final String sha1 = ParamUtil.assertGetStringByName(params, "sha1");

    log.debug("Fetching EVALSHA from Redis for SHA1 sum: {}", sha1);

    return createReturnValue(params, client.evalsha(sha1, keyLength, args));
  }
}
