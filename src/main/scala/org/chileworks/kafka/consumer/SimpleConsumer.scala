package org.chileworks.kafka.consumer

import java.util.Properties

import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.util.KafkaConfig
import org.slf4j.LoggerFactory

class SimpleConsumer(
  val properties: Properties,
  val topics: List[String] = KafkaConfig.TOPICS.split(",").toList.map(_.trim)
) extends TweetConsumer[Tweet] {

  private val logger = LoggerFactory.getLogger(classOf[SimpleConsumer])

  override val maxQueueSize: Int = 1000000

  override def consumeTweet(topic: Long, tweet: Tweet): Tweet = tweet

}
