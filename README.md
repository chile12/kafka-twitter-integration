# kafka-twitter-integration
Simple data-integration pipeline for twitter feeds and knowledge graphs.
Using [Kafka](https://kafka.apache.org), this demo will demonstrate how any Twitter 
feed can be automatically enriched, ether to produce an enriched tweet itself or any other object.
For the purpose of enrichment I have chosen a simple named-entity-recognition framework 
called [DBpedia Spotlight](https://www.dbpedia-spotlight.org) to enrich tweets with NER annotations,
pointing out Wikipedia pages associated with the recognized concept. Any other enrichment data is conceivable as well.
For demonstration purposes I use the Url annotations of the Twitter API to highlight the recognized surface form 
as a link to the Wikipedia page.

Kafka together with Zookeeper as well as Spotlight are Docker based executions and are prerequisite for this demo. 

[https://hub.docker.com/r/wurstmeister/kafka/](https://hub.docker.com/r/wurstmeister/kafka/)

[https://hub.docker.com/r/dbpedia/spotlight-english](https://hub.docker.com/r/dbpedia/spotlight-english)


While this is just a demo project, this is not a simple happy path implementation. Yet, this is noz in a production-ready state.