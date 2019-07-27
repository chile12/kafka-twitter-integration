package org.chileworks.kafka.producers

import java.time
import java.util.Date
import java.util.concurrent.TimeUnit

import org.chileworks.kafka.consumer.{SimpleConsumer, TweetConsumer}
import org.chileworks.kafka.enrichment.FakeEntityEnrichment
import org.chileworks.kafka.model.{Tweet, User}
import org.chileworks.kafka.util.EnrichmentConfig
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success}

class FakeTweetProducerTest extends FunSuite {

  private val defaultWaitTime = Duration.create(10d, TimeUnit.SECONDS)
  private val longWaitTime = Duration.create(20d, TimeUnit.SECONDS)
  private val consumeTime = time.Duration.ofSeconds(10L)
  private val idFactor = 1000000000000000000L
  private val followerFactor = 1000000L
  private val reTweetFactor = 10000L
  private val likeFactor = 100000L
  private val userIdFactor = 10000000L
  private val defaultCollectionSize = 100

  private def random(factor: Long): Long = (Random.nextDouble() * factor).toLong

  val fakeUser1 = User(random(userIdFactor), "Markus Freude", "Chile", "Leeptsch", random(followerFactor).toInt)
  val fakeUser2 = User(random(userIdFactor), "Jan Vorberger", "HolyCrab", "Leeptsch", random(followerFactor).toInt)

  private def collectTweets(consumer: SimpleConsumer, duration: time.Duration): Future[Iterable[Tweet]] = Future{
    val start = new Date().getTime
    var tweets: Iterable[Tweet] = Iterable.empty
    var runningFor = time.Duration.ofMillis(0L)
    // let's give kafka some time
    Thread.sleep(100)
    while (tweets.isEmpty) {
      tweets = consumer.collect(defaultCollectionSize)
      runningFor = time.Duration.ofMillis(new Date().getTime - start)
    }
    tweets.toList
  }

  ignore("round-trip fake tweets"){

    val fakeProducer = new FakeTweetProducer("fake", TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"))

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
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      // here we test if the consumed tweets are all in the list of tweets we just send
      case Success(tweets) =>
        assert(tweets.nonEmpty)
        assert(tweets.forall(tweet => fakeTweets.exists(t => t.id == tweet.id) ))
        tweets
      case Failure(f) => throw new IllegalStateException("No tweets could be collected.", f)
    }
    Await.result(consumeFuture, defaultWaitTime)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    fakeConsumer.close()
  }

  test("round-trip with fake enrichment"){

    val fakeProducer = new FakeTweetProducer("fake", TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simple_consumer"), List(EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC))

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
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      case Success(tweets) =>
        // here we test if the consumed tweets are all in the list of tweets we just send
        assert(tweets.nonEmpty)
        assert(tweets.groupBy(_.id).forall(pair =>{
          pair._2.size == 2 && pair._2.last.text.diff(pair._2.head.text).trim == appendage //TODO
        }))
        tweets
      case Failure(f) =>
        throw new IllegalStateException("No tweets could be collected.", f)
    }
    val res = Await.result(consumeFuture, longWaitTime)
    assert(res.size == fakeTweets.size*2)
    assert(fakeConsumer.queue.isEmpty)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    stream.close()
    fakeConsumer.close()
  }
}
