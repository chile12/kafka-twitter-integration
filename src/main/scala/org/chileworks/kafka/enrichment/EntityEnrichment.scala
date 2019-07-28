package org.chileworks.kafka.enrichment

import java.time.Duration
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.chileworks.kafka.model.{RichTweet, Tweet}
import org.chileworks.kafka.producers.TweetProcessor
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}

/**
  * Enriches a Tweet encountered in a Kafka topic stream based on
  * the implemented [[enrichTweet(tweet: Tweet)]] function and returns it back to Kafka on a different topic.
  *
  * The base enrichment behaviour implemented as a Kafka stream app:
  * consuming objects of [[Tweet]] and producing objects of [[org.chileworks.kafka.model.RichTweet]]
  */
trait EntityEnrichment {

  val properties: Properties

  private val defaultCloseTimeout = Duration.ofSeconds(10L)
  private val gson: Gson = TweetProcessor.getTweetGson
  private var streams: KafkaStreams = _

  /**
    * Will create an instance of [[RichTweet]] based on the input [[Tweet]].
    * @param tweet - the input Tweet
    * @return - the enriched Tweet
    */
  def enrichTweet(tweet: Tweet): RichTweet

  /**
    * Will start the enrichment app. No enrichment is performed before the first call to this method.
    */
  def stream(): Unit ={
    val builder = new StreamsBuilder()
    builder.stream[String, String](EnrichmentConfig.RAW_TOPIC).mapValues{value =>
      if(value.startsWith("{")) {
        val tweet = gson.fromJson(value, RichTweet.determineTweetClass(value))
        val enriched = enrichTweet(tweet)
        gson.toJson(enriched)
      }
      else
        value
    }.to(EnrichmentConfig.RICH_TOPIC)

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
