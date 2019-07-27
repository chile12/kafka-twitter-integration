package org.chileworks.kafka.producers

import java.util.Properties
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.util.logging.Logger

import org.chileworks.kafka.util.KafkaConfig
import org.chileworks.kafka.model.Tweet
import org.apache.commons.lang3.concurrent.ConcurrentUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}
import TwitterFeedProducer._
import org.apache.kafka.clients.consumer.ConsumerConfig

trait TwitterFeedProducer extends KafkaProducer[Long, String] {

  val logger: Logger = Logger.getLogger(id)
  val queueHandle: BlockingQueue[Tweet]
  val tweetProcessor: TweetProcessor
  val topics: List[String] = KafkaConfig.TOPICS.split(",").toList.map(_.trim)
  private lazy val gson = TweetProcessor.getTweetGson

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

  def run(): Future[Unit] = Future{
    while (continueToListen()) {
      Option(queueHandle.poll(TwitterFeedProducer.DEFAULTPOLLTIMEMILLS.toMillis, TimeUnit.MILLISECONDS)) match{
        case Some(tweet) => Try {
          System.out.println("Fetched tweet id %d\n", tweet.id)
          val key = tweet.id
          val msg = gson.toJson(tweet)
          topics.foreach(topic => {
            val record = new ProducerRecord[Long, String](topic, key, msg)
            this.send(record, callback())
          })
        } match{
          case Success(future) => future
          case Failure(failure) =>
            logger.warning(failure.getMessage)
            ConcurrentUtils.constantFuture(null.asInstanceOf[RecordMetadata])
        }
        case None => waitFor(DEFAULTPOLLTIMEMILLS)
      }
    }
    toggleListening()
  }

  def afterRun(): Unit = this.close()

  final def orchestrate(): Future[Unit] = {
    beforeRun()
    run().andThen{
      case _ => afterRun()
    }
  }
}

object TwitterFeedProducer{

  val DEFAULTQUEUESIZE = 10000
  val DEFAULTPOLLTIMEMILLS: FiniteDuration = Duration.create(50L, TimeUnit.MILLISECONDS)

  def waitFor(duration: Duration): Unit ={
    Await.ready(Future{
      Thread.sleep(duration.toMillis - 100)
    }, duration)
  }

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

  def configureConsumer: Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.SERVERS)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.AUTO_OFFSET)
    properties
  }

}
