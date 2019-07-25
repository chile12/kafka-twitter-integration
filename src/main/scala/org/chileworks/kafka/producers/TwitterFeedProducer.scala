package org.chileworks.kafka.producers

import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.logging.Logger

import org.chileworks.kafka.util.{KafkaConfig, TweetSerializer}
import org.chileworks.kafka.model.Tweet
import org.apache.commons.lang3.concurrent.ConcurrentUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.util.{Failure, Success, Try}

trait TwitterFeedProducer extends KafkaProducer[Long, String] {

  val logger: Logger = Logger.getLogger(id)
  val queueHandle: BlockingQueue[String]

  private var _listen = true
  def continueToListen():Boolean = _listen

  def toggleListening(): Unit = {
    _listen = ! _listen
    logger.warning(s"Tweet Producer $id was toggled to further listen: " + _listen )
  }

  def id: String

  private val _defaultCallback = new SimpleCallback()
  def callback(): Callback = _defaultCallback

  def beforeRun(): Unit

  import com.google.gson.Gson
  import com.google.gson.GsonBuilder

  private val gsonBuilder = new GsonBuilder
  gsonBuilder.registerTypeAdapter(classOf[Tweet], new TweetSerializer)
  gsonBuilder.setPrettyPrinting()
  protected val gson: Gson = gsonBuilder.create

  def run(): Unit ={
    while (continueToListen()) {
      Try {
        val tweet = gson.fromJson(queueHandle.take, classOf[Tweet])
        System.out.println("Fetched tweet id %d\n", tweet.id)
        val key = tweet.id
        val msg = tweet.toString
        val record = new ProducerRecord[Long, String](KafkaConfig.TOPIC, key, msg)
        this.send(record, callback())
      } match{
        case Success(future) => future
        case Failure(failure) =>
          logger.warning(failure.getMessage)
          ConcurrentUtils.constantFuture(null.asInstanceOf[RecordMetadata])
      }
    }
  }

  def afterRun(): Unit

  final def orchestrate(): Unit = {
    beforeRun()
    run()
    afterRun()
  }
}

object TwitterFeedProducer{

  val DEFAULTQUEUESIZE = 10000

  def configureProducer: Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.SERVERS)
    properties.put(ProducerConfig.ACKS_CONFIG, "1")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(500))
    properties.put(ProducerConfig.RETRIES_CONFIG, new Integer(0))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties
  }

}
