package org.chileworks.kafka.producers

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.logging.Logger

import org.apache.commons.lang3.concurrent.ConcurrentUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.producers.TwitterFeedProducer._
import org.chileworks.kafka.util.KafkaConfig

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * The base interface for any Tweet producer based on Kafka producer API.
  * This producer is implemented as a forwarder / repeater of tweets to Kafka.
  * Therefore, all tweets to be produced are first entered into a queue
  * to which the producer listens for any new tweets from the actual tweet source.
  * NOTE: the input type could be generic, but String will cover most use cases for now.
  */
trait TwitterFeedProducer extends KafkaProducer[Long, String] {

  /**
    * The name/id of this producer
    */
  val id: String

  private var _listen = true
  private val logger: Logger = Logger.getLogger(id)
  private lazy val gson = TweetProcessor.getTweetGson
  private val queue: BlockingQueue[Tweet] = new LinkedBlockingQueue[Tweet](TwitterFeedProducer.DEFAULTQUEUESIZE)

  /**
    * The processor turning
    */
  val tweetProcessor: TweetProcessor = new TweetProcessor(queue)

  /**
    * The Kafka topics to which all tweets are published.
    */
  val topics: List[String]

  /**
    * Indicates whether or not to allow the ingestion of new Tweets from the source.
    */
  def continueToListen():Boolean = _listen

  /**
    * Will continue listening to new tweets by default until toggled to false.
    */
  def toggleListening(): Unit = {
    _listen = ! _listen
    logger.warning(s"Tweet Producer $id was toggled to further listen: " + _listen )
  }

  /**
    * Will add a new tweet to the queue, which, in turn, will publish the tweet to Kafka eventually.
    * @param tweet - the new tweet
    */
  def publishTweet(tweet: Tweet): Unit = queue.add(tweet)

  private val _defaultCallback = new SimpleCallback()

  /**
    * Override to implement complex callback functions
    */
  def callback(): Callback = _defaultCallback

  /**
    * Executed before the producer is running, to start any dependent processes.
    */
  def beforeRun(): Unit

  /**
    * Will run the Tweet producing activity until stopped by toggling [[toggleListening]]
    */
  def run(): Future[Unit] = Future{
    while (continueToListen()) {
      Option(queue.poll(TwitterFeedProducer.DEFAULTPOLLTIMEMILLS.toMillis, TimeUnit.MILLISECONDS)) match{
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

  /**
    * Will be executed after the Tweet producing activity has stopped, to clean up the place.
    */
  def afterRun(): Unit = this.close()

  /**
    * The orchestration function to start up, execute and shut down the Tweet publishing activity.
    * @return
    */
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
}
