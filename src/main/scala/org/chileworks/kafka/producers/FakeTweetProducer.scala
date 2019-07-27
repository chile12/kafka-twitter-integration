package org.chileworks.kafka.producers

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

class FakeTweetProducer(val id: String, properties: Properties) extends KafkaProducer[Long, String](properties) with TwitterFeedProducer {

  override def beforeRun(): Unit = Unit

  override def afterRun(): Unit = this.close()
}
