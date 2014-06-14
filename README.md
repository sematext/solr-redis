[![Build Status](https://travis-ci.org/sematext/solr-redis.svg)](https://travis-ci.org/sematext/solr-redis)

Solr Redis Extensions 
=====================


This extenstion is a ParserPlugin that provides a Solr query parser based on data stored in Redis.

RedisQParserPlugin creates a connection with Redis and passes the connection object to RedisQParser responsible for fetching data and building a query.

## Quick start

Build with Maven:

```shell
mvn install
```

Put the library (solr-redis-*.jar) in Solr lib directory ($SOLR_HOME/lib). 

Configure the query parser plugin in solrconfig.xml. Add the following to the "config" section of solrconfig.xml:

```xml
<queryParser name="redis" class="com.sematext.solr.redis.RedisQParserPlugin">
  <str name="host">localhost</str>
  <str name="maxConnections">30</str>
  <str name="retries">2</str>
</queryParser>
```

## Usage

### Allowed parameters for the RedisQParserPlugin:

 * **method** - Method for Redis. Currently allowed: smembers, zrevrangebyscore (required)
 * **key** - Key used to fetch data from Redis (required)
 * **operator** - Operator which connects terms taken from Redis. Allowed values are AND/OR (optional - default is OR)
 * **useAnalyzer** - Turns on and off query time analyzer true/false (optinal - default is true)
 
ZREVRANGEBYSCORE specific parameters: 
 * **min** - Minimal value of range (optional)
 * **key** - Maximal value of range (required)

Examples of usage: 
 * q=\*:\*&fq={!redis method=smembers key=some_key}field
 * q=\*:\*&fq={!redis method=zrevrangebyscore key=some_key min=1 max=1000}field
 * q=\*:\*&fq={!redis method=zrevrangebyscore key=some_key max=213}field

### Allowed configuration parameter for RedisQParserPlugin (section in solrconfig.xml):
 * **host** - Host of the Redis server (can also be host:port)
 * **maxConnections** - Maximal number of connections to Redis
 * **retries** - Number of retries before Redis command fails
 * **database** - Redis database ID
 * **password** - Redis password
 * **timeout** - Redis client read timeout
