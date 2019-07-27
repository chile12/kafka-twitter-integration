package org.chileworks.kafka.consumer

import java.time.Duration
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.producers.TweetProcessor
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SimpleConsumer(properties: Properties) extends KafkaConsumer[Long, String](properties) {

  private val logger = LoggerFactory.getLogger(classOf[SimpleConsumer])
  private val gson: Gson = TweetProcessor.getTweetGson

  def pollTweets(duration: Duration): Iterable[Tweet] = poll(duration).asScala.flatMap{ record =>
    Try{gson.fromJson(record.value(), classOf[Tweet])} match{
      case Success(tweet) =>
        commitAsync()
        Some(tweet)
      case Failure(f) =>
        logger.warn("Could not parse tweet: " + record.value())
        None
    }
  }
}
