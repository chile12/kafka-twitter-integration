package org.chileworks.kafka.producers

import org.chileworks.kafka.TweetGeneratorUtil._
import org.chileworks.kafka.consumer.{SimpleConsumer, TweetConsumer}
import org.chileworks.kafka.enrichment.FakeEntityEnrichment
import org.chileworks.kafka.model
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}

class FakeTweetProducerTest extends FunSuite {

  private val fakeUser1 = createUser("Markus Freude", "Chile", "Leeptsch")
  private val fakeUser2 = createUser("Jan Vorberger", "HolyCrab", "Leeptsch")

  test("round-trip fake tweets"){

    val fakeProducer = new FakeTweetProducer("fake", KafkaConfig.TOPICS.split(",").toList.map(_.trim), TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"))
    fakeConsumer.start()

    val fakeTweets = Seq(
      createTweet(fakeUser2, "@Chile you can't code, man!", "en"),
      createTweet(fakeUser1, "Al least I don't suck! @HolyCrab", "en"),
      createTweet(fakeUser2, "In your dreams...", "en")
    )
    // start the twitter orchestration worker
    val produceFuture = fakeProducer.orchestrate()
    // queue fake tweets
    fakeTweets.foreach(fakeProducer.publishTweet)
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

    val fakeProducer = new FakeTweetProducer("fake", KafkaConfig.TOPICS.split(",").toList.map(_.trim), TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"), List(EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC))
    fakeConsumer.start()

    val appendage = "___TEST___"

    val fakeTweets = Seq(
      createTweet(fakeUser2, "Oi, this is kinda easy.", "en"),
      createTweet(fakeUser1, "Meh", "en")
    )

    // creating a fake enrichment factory: sentiment is random, and some pointless text pointers
    val stream = new FakeEntityEnrichment(appendage, (_: Tweet) => Random.nextFloat(), Map(
      23 -> model.UrlObj("https://en.wikipedia.org/wiki/Hamburg", "https://en.wikipedia.org/wiki/Hamburg", "Hamburg", (23, 30))
    ))
    stream.stream()

    // start the twitter orchestration worker
    val produceFuture = fakeProducer.orchestrate()
    // queue fake tweets
    fakeTweets.foreach(fakeProducer.publishTweet)
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      case Success(tweets) =>
        // here we test if the consumed tweets are all in the list of tweets we just send
        assert(tweets.nonEmpty)
        assert(tweets.groupBy(_.id).forall(pair =>{
          pair._2.size == 2 && pair._2.forall(t => t.entitiesObj.nonEmpty)
        }))
        tweets
      case Failure(f) =>
        throw new IllegalStateException("No tweets could be collected.", f)
    }
    val res = Await.result(consumeFuture, longWaitTime)
    assert(res.size == fakeTweets.size*2)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    stream.close()
    fakeConsumer.close()
  }
}
