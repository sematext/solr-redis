[![Build Status](https://travis-ci.org/sematext/solr-redis.svg)](https://travis-ci.org/sematext/solr-redis)

Solr Redis Extensions 
=====================


That extenstion is ParserPlugin which builds a query parser basing on data stored in Redis.

RedisQParserPlugin initiates connection with Redis and pass the connection object to RedisQParser which is responsible for fetching data and building a query.

## Quick start

When you build the library by Maven:

```shell
mvn install
```

You should put the library (solr-redis-*.jar) into solr lib directory ($SOLR_HOME/lib). Then query parser plugin should be configured in solrconfig.xml. You have to put the lines presented below in "config" section of solrconfig.xml:

```xml
<queryParser name="redis" class="com.sematext.solr.redis.RedisQParserPlugin">
  <str name="host">localhost</str>
  <str name="maxConnections">30</str>
  <str name="retries">2</str>
</queryParser>
```

## Usage

### Allowed parameters for the RedisQParserPlugin:

 * **method** - Method for Redis. Currently allowed: smembers, zrange, zrevrange (required)
 * **key** - Key used to fetch data from Redis (required)
 * **operator** - Operator which connects terms taken from Redis. Allowed values are "AND" and "OR". Default operator is OR (optional)
 
ZRANGE and ZREVRANGE specific parameters: 
 * **min** - Minimum value of range (optional)
 * **key** - Maximum value of range (required)

Examples of usage: 
 * q=\*:\*&fq={!redis method=smembers key=some_key}field
 * q=\*:\*&fq={!redis method=zrevrangebyscore key=some_key min=1 max=1000}field
 * q=\*:\*&fq={!redis method=zrevrangebyscore key=some_key max=213}field

### Allowed configuration parameter of RedisQParserPlugin (secrion in solrconfig.xml):
 * **host** - Host of redis server (can be also host:port)
 * **maxConnections** - Maximum number of connections to Redis
 * **retries** - Number of retries when redis command fails
 * **database** - Redis database id
 * **password** - Password to Redis
 * **timeout** - Read timeout for Redis client
