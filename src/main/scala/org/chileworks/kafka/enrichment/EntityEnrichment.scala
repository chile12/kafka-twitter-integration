package org.chileworks.kafka.enrichment

import java.time.Duration
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.producers.TweetProcessor
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}

/**
  * Enriches a Tweet encountered in a Kafka topic stream based on
  * the implemented [[enrichTweet(tweet: Tweet)]] function and returns it back to Kafka on a different topic.
  *
  * The base enrichment behaviour implemented as a Kafka stream app:
  * consuming objects of [[Tweet]] and including [[EntityObj]] es enrichment
  */
trait EntityEnrichment[T] {

  val properties: Properties

  private val defaultCloseTimeout = Duration.ofSeconds(10L)
  private val gson: Gson = TweetProcessor.getTweetGson
  private var streams: KafkaStreams = _

  /**
    * Will create an instance of [[T]] based on the input [[Tweet]].
    * @param tweet - the input Tweet
    * @return - the enriched T
    */
  def enrichTweet(tweet: Tweet): T

  /**
    * The Kafka topic to which a Twitter feed has been directly forwarded
    */
  val rawTopic: String = EnrichmentConfig.RAW_TOPIC

  /**
    * The Kafka Topic into which the enriched tweets are fed
    */
  val richTopic: String = EnrichmentConfig.RICH_TOPIC

  /**
    * Will start the enrichment app. No enrichment is performed before the first call to this method.
    */
  def stream(): Unit ={
    val builder = new StreamsBuilder()
    builder.stream[String, String](rawTopic).mapValues{value =>
      if(value.startsWith("{")) {
        val tweet: Tweet = gson.fromJson(value, Tweet.typeOfSrc)
        val enriched = enrichTweet(tweet)
        gson.toJson(enriched)
      }
      else
        value
    }.to(richTopic)

    streams = new KafkaStreams(builder.build, properties)
    streams.start()
  }

  /**
    * Will end and close the the enrichment stream.
    */
  def close(): Unit = {
    streams.close(defaultCloseTimeout)
    streams.cleanUp()
  }
}

object EntityEnrichment{
  val RawTopicConf = "raw_topic"
  val RichTopicConf = "rich_topic"
  val StreamAppConf = "stream_app"

  def configureStream: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, EnrichmentConfig.STREAM_APP)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.SERVERS)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties
  }
}
