# kafka-connect-elasticsearch-source
Kafka Connect Elasticsearch Source: fetch data from elasting indices using scroll API. Fetch only new data using a strictly incremental / temporal field.
 It supports dynamic schema and nested objects/ arrays.

## Installation:
1. ``mvn clean package -DskipTests``
2. Copy (`target/elastic-source-connect-0.6-jar-with-dependencies.jar`) the jar and put into the connect classpath (e.g ``/usr/share/java/kafka-connect-elasticsearch`` ) or set ``plugin.path`` parameter appropriately.

## Example
Using kafka connect in distributed way, a sample config file to fetch ``metric*`` indices and to produce output topics with ``es_`` prefix:


```json
{       "name": "elastic-source",
    "config": {"connector.class":"com.github.dariobalinzo.ElasticSourceConnector",
                                "tasks.max": "1",
                                "es.host" : "localhost",
                                "es.port" : "9200",
                                "index.prefix" : "metric",
                                "topic.prefix" : "es_",
                                "incrementing.field.name" : "@timestamp"
        }
}
```
To start the connector we send the json config with post:
```bash
curl -X POST -H "Content-Type: application/json" --data @config.json http://localhost:8083/connectors | jq
  ```

To check the status:
```bash
curl localhost:8083/connectors/elastic-source/status | jq
  ```


## Documentation

### Elasticsearch Configuration

``es.host``
  ElasticSearch host

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``

``es.port``
  ElasticSearch port

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``

``es.scheme``
  ElasticSearch scheme

  * Type: string
  * Importance: medium
  * Default: ``http``
  
``es.user``
  Elasticsearch username

  * Type: string
  * Default: null
  * Importance: high

``es.password``
  Elasticsearch password

  * Type: password
  * Default: null
  * Importance: high

``connection.attempts``
  Maximum number of attempts to retrieve a valid Elasticsearch connection.

  * Type: int
  * Default: 3
  * Importance: low

``connection.backoff.ms``
  Backoff time in milliseconds between connection attempts.

  * Type: long
  * Default: 10000
  * Importance: low

``index.prefix``
  Indices prefix to include in copying.

  * Type: string
  * Default: ""
  * Importance: medium
 
 ``is.index.alias.prefix``
   Is search alias index 
 
   * Type: string
   * Default: "false"
   * Importance: medium
   
 ``index.mapping.type``
    Mapping type index
    
    * Type: string
    * Default: ""
    * Importance: medium 
    
 ``whitelist.fields``
    Allow extracts fields
    
    * Type: string
    * Default: ""
    * Importance: medium
   
``cast.string.fields``
   Allow cast array, map, struct fields to string
   
   * Type: string
   * Default: ""
   * Importance: medium

### Connector Configuration

``poll.interval.ms``
  Frequency in ms to poll for new data in each index.

  * Type: int
  * Default: 5000
  * Importance: high

``batch.max.rows``
  Maximum number of documents to include in a single batch when polling for new data.

  * Type: int
  * Default: 10000
  * Importance: low

``topic.prefix``
  Prefix to prepend to index names to generate the name of the Kafka topic to publish data

  * Type: string
  * Importance: high
  
