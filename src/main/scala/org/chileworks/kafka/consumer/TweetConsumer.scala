package org.chileworks.kafka.consumer

import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.chileworks.kafka.model.{RichTweet, Tweet}
import org.chileworks.kafka.producers.TweetProcessor
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Consumes one or more topics and stores the resulting transformed objects in a queue for further procession.
  *
  * The TweetConsumer is based on the Kafka Stream API (and not the consumer API)
  * since periodic polling introduces additional latency and a constant offset enforcement.
  * The downside it that you have to keep the encountered events in a Queue and have to
  * take care of maintenance and overflow behaviour (which is not covered by this test project.
  * @tparam T - the type into which a received event is transformed (in this demo, this should be Tweets)
  */
trait TweetConsumer[T] {

  private val defaultCloseTimeout = Duration.ofSeconds(10L)
  private val logger = LoggerFactory.getLogger(classOf[TweetConsumer[T]])
  protected val gson: Gson = TweetProcessor.getTweetGson

  /**
    * The topic list, each of which will be observed and their Tweets collected in the queue.
    */
  val topics: List[String]

  /**
    * The queue may only consist of this many elements.
    * Exceeding this number will trigger the queue o
    */
  val maxQueueSize: Int

  // the queue
  private val queue = new ConcurrentLinkedQueue[T]()
  private val queueSize = new AtomicInteger(0)

  val properties: Properties

  private val streams: KafkaStreams = new KafkaStreams(createStream, properties)

  private def addToQueue(id: Long, tweet: Tweet): Unit ={
    val t = consumeTweet(id, tweet)
    val size = queueSize.incrementAndGet()
    if(size >= maxQueueSize){
      queueOverflowBehaviour(queue, t)
    }
    else{
      queue.add(t)
    }
  }

  private def createStream = {
    //// initialize stream collection
    val builder = new StreamsBuilder()
    builder.stream[Long, String](topics.asJava).foreach { (id: Long, value: String) =>
      if (value.startsWith("{")) {
        val tweet = gson.fromJson(value, RichTweet.determineTweetClass(value))
        addToQueue(id, tweet)
      }
    }
    builder.build
  }
  // start the streaming by default
  streams.start()

  /**
    * Will be triggered when the Queue is about to overflow (exeeding the define [[maxQueueSize]]).
    * @param q - the queue in question
    * @param latest - the element which would exceed the max size
    */
  def queueOverflowBehaviour(q: ConcurrentLinkedQueue[T], latest: T): Unit = {
    // TODO outside of scope of this demo
    logger.warn("Exceeding queue size for TweetConsumer. The following object is discarded: " + latest.toString)
  }

  /**
    * For each Tweet encountered, this function will be executed to derive the desired target type from the tweet.
    * @param id - the id of the tweet
    * @param tweet - the tweet
    * @return - the target type created by transforming the input tweet
    */
  def consumeTweet(id: Long, tweet: Tweet): T

  /**
    * Will collect all objects created since the last call of this function
    * @param maxElements - will only collect this many objects at once
    * @return - a sequence of T
    */
  def collect(maxElements: Int): Iterable[T] = {
    queue.iterator().asScala.take(maxElements)
      .map(q => {
        queue.remove(q)
        queueSize.decrementAndGet()
        q
      }).toSeq
  }

  /**
    * Finalize consumer
    */
  def close(): Unit = {
    streams.close(defaultCloseTimeout)
    streams.cleanUp()
  }

}

object TweetConsumer{

  def configureConsumer(groupName: String = EnrichmentConfig.STREAM_APP): Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, groupName)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.SERVERS)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties
  }
}
