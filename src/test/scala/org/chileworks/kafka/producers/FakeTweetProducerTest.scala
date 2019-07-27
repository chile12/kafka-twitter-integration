package org.chileworks.kafka.producers

import java.time
import java.util.Date
import java.util.concurrent.TimeUnit

import org.chileworks.kafka.consumer.SimpleConsumer
import org.chileworks.kafka.enrichment.FakeEntityEnrichment
import org.chileworks.kafka.model.{Tweet, User}
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.util.{Failure, Random, Success}

class FakeTweetProducerTest extends FunSuite {

  private val defaultWaitTime = Duration.create(10d, TimeUnit.SECONDS)
  private val consumeTime = time.Duration.ofSeconds(10L)
  private val idFactor = 1000000000000000000L
  private val followerFactor = 1000000L
  private val reTweetFactor = 10000L
  private val likeFactor = 100000L
  private val userIdFactor = 10000000L

  private def random(factor: Long): Long = (Random.nextDouble() * factor).toLong

  val fakeUser1 = User(random(userIdFactor), "Markus Freude", "Chile", "Leeptsch", random(followerFactor).toInt)
  val fakeUser2 = User(random(userIdFactor), "Jan Vorberger", "HolyCrab", "Leeptsch", random(followerFactor).toInt)

  private def collectTweets(consumer: SimpleConsumer, duration: time.Duration): Future[Iterable[Tweet]] = Future{
    val start = new Date().getTime
    var tweets: Iterable[Tweet] = Iterable.empty
    var runningFor = time.Duration.ofMillis(0L)
    // let's give kafka some time
    Thread.sleep(100)
    while (tweets.isEmpty || runningFor.compareTo(duration) < 0) {
      tweets = consumer.pollTweets(duration)
      runningFor = time.Duration.ofMillis(new Date().getTime - start)
    }
    tweets
  }

  ignore("round-trip fake tweets"){

    val fakeProducer = new FakeTweetProducer("fake", TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TwitterFeedProducer.configureConsumer)
    fakeConsumer.subscribe(KafkaConfig.TOPICS.split(",").toList.map(_.trim).asJava)

    val now = new Date().getTime
    val fakeTweets = Seq(
      Tweet(random(idFactor), "@Chile you can't code, man!", "en", fakeUser2, random(reTweetFactor).toInt, random(likeFactor).toInt, now),
      Tweet(random(idFactor), "Al least I don't suck! @HolyCrab", "en", fakeUser1, random(reTweetFactor).toInt, random(likeFactor).toInt, now),
      Tweet(random(idFactor), "In your dreams...", "en", fakeUser1, random(reTweetFactor).toInt, random(likeFactor).toInt, now)
    )
    // start the twitter orchestration worker
    val produceFuture = fakeProducer.orchestrate()
    // queue fake tweets
    fakeTweets.foreach(fakeProducer.fakeProduce)

    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      // here we test if the consumed tweets are all in the list of tweets we just send
      case Success(tweets) =>
        assert(tweets.nonEmpty)
        assert(tweets.forall(tweet => fakeTweets.exists(t => t.id == tweet.id) ))
        tweets
      case Failure(f) => throw new IllegalStateException("No tweets could be collected.", f)
    }
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    Await.result(consumeFuture, defaultWaitTime)
  }

  test("round-trip with fake enrichment"){

    val fakeProducer = new FakeTweetProducer("fake", TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TwitterFeedProducer.configureConsumer)
    val topicsToObserve = List(EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC)
    fakeConsumer.subscribe(topicsToObserve.asJava)

    val appendage = "___TEST___"

    val now = new Date().getTime
    val fakeTweets = Seq(
      Tweet(random(idFactor), "Oi, this is kinda easy.", "en", fakeUser2, random(reTweetFactor).toInt, random(likeFactor).toInt, now),
      Tweet(random(idFactor), "Meh", "en", fakeUser1, random(reTweetFactor).toInt, random(likeFactor).toInt, now)
    )

    // creating a fake enrichment factory: sentiment is random, and some pointless text pointers
    val stream = new FakeEntityEnrichment(appendage, (_: Tweet) => Random.nextFloat(), Map(
      23 -> "skjgfdsjk",
      34 -> "hdsfsf",
      104 -> "hfdjsgvhr"
    ))
    stream.stream()

    // start the twitter orchestration worker
    val produceFuture = fakeProducer.orchestrate()
    // queue fake tweets
    fakeTweets.foreach(fakeProducer.fakeProduce)
    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      case Success(tweets) =>
        // here we test if the consumed tweets are all in the list of tweets we just send
        assert(tweets.nonEmpty)
        assert(tweets.groupBy(_.id).forall(pair =>{
          pair._2.size == 2 && pair._2.head.text.diff(pair._2.last.text) == appendage
        }))
        tweets
      case Failure(f) => throw new IllegalStateException("No tweets could be collected.", f)
    }
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    Await.result(consumeFuture, defaultWaitTime)
    stream.close()
  }
}
