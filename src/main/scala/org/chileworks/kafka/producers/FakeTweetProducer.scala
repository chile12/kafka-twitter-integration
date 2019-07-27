package org.chileworks.kafka.producers

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.chileworks.kafka.model.Tweet
import org.apache.kafka.clients.producer.KafkaProducer

class FakeTweetProducer(val id: String, properties: Properties) extends KafkaProducer[Long, String](properties) with TwitterFeedProducer {

  def fakeProduce(tweet: Tweet): Unit ={
    queueHandle.add(tweet)
  }

  override val queueHandle: BlockingQueue[Tweet] = new LinkedBlockingQueue[Tweet](TwitterFeedProducer.DEFAULTQUEUESIZE)

  override def beforeRun(): Unit = Unit

  override def afterRun(): Unit = this.close()

  override val tweetProcessor: TweetProcessor = new TweetProcessor(queueHandle)
}
