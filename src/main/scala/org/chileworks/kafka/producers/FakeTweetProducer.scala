package org.chileworks.kafka.producers

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.chileworks.kafka.model.Tweet
import org.apache.kafka.clients.producer.KafkaProducer

class FakeTweetProducer(val id: String) extends KafkaProducer[Long, String](TwitterFeedProducer.configureProducer) with TwitterFeedProducer {

  def fakeProduce(tweet: Tweet): Unit ={
    queueHandle.add(gson.toJson(tweet))
  }

  override val queueHandle: BlockingQueue[String] = new LinkedBlockingQueue[String](TwitterFeedProducer.DEFAULTQUEUESIZE)

  override def beforeRun(): Unit = Unit

  override def afterRun(): Unit = Unit
}
