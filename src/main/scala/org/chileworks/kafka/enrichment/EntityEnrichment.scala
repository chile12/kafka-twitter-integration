package org.chileworks.kafka.enrichment

import java.time.Duration
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.producers.TweetProcessor
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}

trait EntityEnrichment {

  val properties: Properties

  private val defaultCloseTimeout = Duration.ofSeconds(10L)
  private val gson: Gson = TweetProcessor.getTweetGson
  private var streams: KafkaStreams = _

  def transformTweet(tweet: Tweet): Tweet

  def stream(): Unit ={
    val builder = new StreamsBuilder()
    builder.stream[String, String](EnrichmentConfig.RAW_TOPIC).mapValues{value =>
      if(value.startsWith("{")) {
        val tweet = gson.fromJson(value, classOf[Tweet])
        val enriched = transformTweet(tweet)
        gson.toJson(enriched)
      }
      else
        value
    }.to(EnrichmentConfig.RICH_TOPIC)

    streams = new KafkaStreams(builder.build, properties)
    streams.start()
  }

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
