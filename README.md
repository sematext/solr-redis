[![Build Status](https://travis-ci.org/sematext/solr-redis.svg)](https://travis-ci.org/sematext/solr-redis)

Solr Redis Extensions
=====================


This extension is a ParserPlugin that provides a Solr query parser based on data stored in Redis.

RedisQParserPlugin creates a connection with Redis and passes the connection object to RedisQParser responsible for fetching data and building a query.

## Quick start

Build with Maven:

```shell
mvn install
```

Put the library (`solr-redis-*.jar`) in Solr lib directory (`$SOLR_HOME/lib`).

Configure the query parser plugin in `solrconfig.xml`. Add the following to the "config" section of `solrconfig.xml`:

```xml
<queryParser name="redis" class="com.sematext.solr.redis.RedisQParserPlugin">
  <str name="host">localhost</str>
  <str name="maxConnections">30</str>
  <str name="retries">2</str>
</queryParser>
```

## Usage

### Allowed parameters for the RedisQParserPlugin:

 * **command** - Redis command. Currently allowed: `SMEMBERS`, `SRANDMEMBER`, `SDIFF`, `SINTER`, `SUNION`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `HKEYS`, `HMGET`, `HVALS`, `LRANGE`, `LINDEX`, `GET`, `MGET`, `KEYS`, `SORT`, `EVAL`, `EVALSHA` (required)
 * **key** - Key used to fetch data from Redis (required)
 * **operator** - Operator which connects terms taken from Redis. Allowed values are AND/OR (optional - default is OR)
 * **useAnalyzer** - Turns on and off query time analyzer true/false (optional - default is true)

GET specific parameters:
 * **compression**: Defines a format for compression. `gzip` is the only supported option right now
 * **serialization**: Defines an format for deserialization. `json` is the only supported option right now and assumes to unpack the JSON payload as a list of strings

SRANDMEMBER specific parameters:
 * **count** - Number of random keys to retrieve (default `1`)

SDIFF, SINTER and SUNION specific parameters;
 * **key\*** - Anything that starts with the prefix **key** is assumed to be a key for the union

ZRANGEBYSCORE and ZREVRANGEBYSCORE specific parameters:
 * **min** - Minimal value of range (optional, default `-inf`)
 * **max** - Maximal value of range (optional, default `+inf`)

ZRANGE and ZREVRANGE specific parameters:
 * **range_start** - Start value of the range (optional, default `0`)
 * **range_end** - End value of the range (optional, default `-1`)

HGET specific parameters:
 * **field** - Field name of the hash (required)

HMGET specific parameters:
 * **field\*** - Anything that starts with the prefix **field** is assumed to be a hash field for the multi field lookup

LRANGE specific parameters:
 * **min** - Minimal value of range (optional, default `0`)
 * **max** - Maximal value of range (optional, default `-1`)

LINDEX specific parameters:
 * **index** - The index that should be returned from the list (default `0`)

MGET specific parameters:
 * **key\*** - Anything that starts with the prefix **key** is assumed to be a key for the multi key lookup

SORT specific parameters:
 * **offset** - Specify an offset
 * **limit** - Specify a limit
 * **order** - Specify an order direction (either "desc" or "asc")
 * **algorithm** - Sort algorithm to apply. Pass "alpha" to select alphanumeric sorting (default empty)
 * **by** - Specify a different sort keys
 * **get\*** - Anything that starts with the prefix **get** is assumed to a redis SORT GET parameter

EVAL specific parameters:
 * **script** - Specify the LUA script to run
 * **returns_hash** - Specify if a LUA table like the following should be treated as a hash: `{1.2, 'str1', 2.2, 'str2'}`
 * **key\*** - Anything that starts with the prefix **key** is assumed to be a key
 * **arg\*** - Anything that starts with the prefix **arg** is assumed to be an arg

EVALSHA specific parameters:
 * **sha1** - Specify the SHA1 hash of the stored script
 * **returns_hash** - Specify if a LUA table like the following should be treated as a hash: `{1.2, 'str1', 2.2, 'str2'}`
 * **key\*** - Anything that starts with the prefix **key** is assumed to be a key
 * **arg\*** - Anything that starts with the prefix **arg** is assumed to be an arg

Examples of usage:
 * `q=*:*&fq={!redis command=SMEMBERS key=some_key}field`
 * `q=*:*&fq={!redis command=SRANDMEMBER key=some_key count=2}field`
 * `q=*:*&fq={!redis command=SDIFF key=set0 key1=set1}field`
 * `q=*:*&fq={!redis command=SINTER key=set0 key1=set1}field`
 * `q=*:*&fq={!redis command=SUNION key=set0 key1=set1}field`
 * `q=*:*&fq={!redis command=HVALS key=some_key}field`
 * `q=*:*&fq={!redis command=HGET key=some_key field=f1}field`
 * `q=*:*&fq={!redis command=HMGET key=some_key field0=f1 field1=f2}field`
 * `q=*:*&fq={!redis command=HKEYS key=some_key}field`
 * `q=*:*&fq={!redis command=ZRANGE key=some_key range_start=1 range_end=1000}field`
 * `q=*:*&fq={!redis command=ZREVRANGE key=some_key range_start=1 range_end=1000}field`
 * `q=*:*&fq={!redis command=ZRANGEBYSCORE key=some_key min=1 max=1000}field`
 * `q=*:*&fq={!redis command=ZREVRANGEBYSCORE key=some_key max=213}field`
 * `q=*:*&fq={!redis command=LRANGE key=list_key min=1 max=-1}`
 * `q=*:*&fq={!redis command=LINDEX key=list_key index=-1}`
 * `q=*:*&fq={!redis command=GET key=redis_key}`
 * `q=*:*&fq={!redis command=MGET key=key_one key1=key_two key2=key_three}`
 * `q=*:*&fq={!redis command=KEYS key=pattern}`
 * `q=*:*&fq={!redis command=SORT key=key_one algorithm=alpha offset=1 limit=100 by=weight_* get0=obj1_* get1=obj2_*}`

### Allowed configuration parameter for RedisQParserPlugin (section in solrconfig.xml):
 * **host** - Host of the Redis server (can also be host:port)
 * **maxConnections** - Maximal number of connections to Redis
 * **retries** - Number of retries before Redis command fails
 * **database** - Redis database ID
 * **password** - Redis password
 * **timeout** - Redis client read timeout

## Highlighting

SolrRedis plugin is able to highlight matching parts of documents which are used in redis
query. Highlighting used in SolrRedis plugin is fully compatible with default Solr
highlighter and supports all parameters passed to Solr highlighter.

### Configuration

To use highlighter it has to be configured in `solrconfig.xml` file. SolrRedis highlighter uses class TaggedqueryHighlighter which is responsible for highlighting document parts matched to Redis query.

```xml
<searchComponent class="solr.HighlightComponent" name="highlight">
   <highlighting class="com.sematext.solr.highlighter.TaggedQueryHighlighter"/>
</searchComponent>
```



### Usage

You have to remember that highlighting will work only for string and text
fields due to issued in Lucene/Solr
([SOLR-2497](https://issues.apache.org/jira/browse/SOLR-2497),
[LUCENE-3080](https://issues.apache.org/jira/browse/LUCENE-3080),
[SOLR-3050](https://issues.apache.org/jira/browse/SOLR-3050)).



```
curl 'http://localhost:8983/solr/collection1/select?q={!redis command=SMEMBERS key=testKey tag=highlightedField}test_field&hl=true&hl.fl=highlightedField}'
```

**Please note** that field used in `tag` parameter of SolrRedis QParser doesn't
have to exist in schema. It is only virtual (tag) field. This is only used to identify
matches of which query should be highlighted.

### Response

Matching fragments are included in a special section of the response (highlighting section).
Client uses the formatting clues (pre and post) also included to determine how to present
the snippets to users.

Example of response:

```xml
<response>
  <lst name="responseHeader">
    <int name="status">0</int>
    <int name="QTime">10</int>
    <lst name="params">
      <str name="q">
        {!redis command=SMEMBERS key=testKey tag=highlightedField}test_field
      </str>
      <str name="hl.fl">highlightedField</str>
      <str name="hl">true</str>
    </lst>
  </lst>
  <result name="response" numFound="1" start="0">
    <doc>
      <str name="id">123</str>
      <str name="test_field">highlighterContent</str>
      <long name="_version_">1487238265204375552</long>
    </doc>
  </result>
  <lst name="highlighting">
    <lst name="123">
      <arr name="highlightedField">
        <str><em>highlighterContent</em></str>
      </arr>
    </lst>
  </lst>
</response>
```

### More complex query examples

```
curl 'http://localhost:8983/solr/collection1/select?q= \
({!redis command=SMEMBERS key=testKey1 tag=alias1}test_field) OR\
({!redis command=SMEMBERS key=testKey2 tag=alias2}test_field)\
&hl=true&hl.fl=alias1,alias2}
```

**Please note** that each Query parser statement ({!redis ....}) should be placed between brackets.

You can also mix SolrRedis highlighting with default Solr highlighting:

```
curl 'http://localhost:8983/solr/collection1/select?q=
({!redis command=SMEMBERS key=testKey1 tag=alias1}test_field) OR (foo:bar) &hl=true&hl.fl=alias1,foo}
```

Response will be as below

```xml
<response>
  <lst name="responseHeader">
    <int name="status">0</int>
    <int name="QTime">10</int>
    <lst name="params">
      <str name="q">
        {!redis command=SMEMBERS key=testKey1 tag=alias1}test_field OR (foo:bar)
      </str>
      <str name="hl.fl">alias1,foo</str>
      <str name="hl">true</str>
    </lst>
  </lst>
  <result name="response" numFound="2" start="0">
    <doc>
      <str name="id">1</str>
      <str name="test_field">test</str>
      <str name="foo">baz</str>
      <long name="_version_">1487238265204375552</long>
    </doc>
    <doc>
      <str name="id">2</str>
      <str name="test_field">not_test</str>
      <str name="foo">bar</str>
      <long name="_version_">1487238265204375552</long>
    </doc>
  </result>
  <lst name="highlighting">
    <lst name="1">
      <arr name="alias1">
        <str><em>test</em></str>
      </arr>
    </lst>
    <lst name="2">
      <arr name="foo">
        <str><em>bar</em></str>
      </arr>
    </lst>
  </lst>
</response>
```
