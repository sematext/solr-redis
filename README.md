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

 * **command** - Redis command. Currently allowed: `SMEMBERS`, `SRANDMEMBER`, `SDIFF`, `SINTER`, `SUNION` `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `HKEYS`, `HMGET`, `HVALS`, `LRANGE`, `LINDEX`, `GET`, `MGET`, `KEYS`, `SORT` (required)
 * **key** - Key used to fetch data from Redis (required)
 * **operator** - Operator which connects terms taken from Redis. Allowed values are AND/OR (optional - default is OR)
 * **useAnalyzer** - Turns on and off query time analyzer true/false (optional - default is true)

SRANDMEMBER specific parameters:
 * **count** - Number of random keys to retrieve (default `1`)

SDIFF, SINTER and SUNION specific parameters;
 * **key\*** - Anything that starts with the prefix **key** is assumed to be a key for the union

ZRANGEBYSCORE and ZREVRANGEBYSCORE specific parameters:
 * **min** - Minimal value of range (optional, default `-inf`)
 * **max** - Maximal value of range (optional, default `+inf`)
 
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
