package org.chileworks.kafka.producers

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Can be used to feed false Tweets into a Kafka topic stream
  * @param id - the name
  * @param properties - Properties
  */
class FakeTweetProducer(val id: String, val topics:List[String], properties: Properties) extends KafkaProducer[Long, String](properties) with TwitterFeedProducer {

  override def beforeRun(): Unit = Unit

  override def afterRun(): Unit = this.close()
}
