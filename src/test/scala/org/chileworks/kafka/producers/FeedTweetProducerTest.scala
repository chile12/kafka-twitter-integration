package org.chileworks.kafka.producers

import java.time
import java.util.concurrent.TimeUnit

import org.chileworks.kafka.TweetGeneratorUtil._
import org.chileworks.kafka.consumer.{SimpleConsumer, TweetConsumer}
import org.chileworks.kafka.enrichment.{EntityEnrichment, NamedEntityEnrichment}
import org.chileworks.kafka.util.TwitterConfig
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FeedTweetProducerTest extends FunSuite {
  val longWaitTime: Duration = Duration.create(5d, TimeUnit.MINUTES)
  val twitterConsumeTime: time.Duration = time.Duration.ofSeconds(60L)
  val twitterWaitTime: Duration = Duration.create(twitterConsumeTime.getSeconds + 20L, TimeUnit.SECONDS)

  // NOTE: this test is not really for testing the application but for trying out the enrichment on real twitter feeds
  test("collect tweets from given hashtags"){

    val twitterProducer = new FeedTweetProducer("hamburg", List(TwitterConfig.TARGET_KAFKA_TOPIC), TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"), List(TwitterConfig.ENRICHED_KAFKA_TOPIC))
    fakeConsumer.start()

    // creating a fake enrichment factory: sentiment is random, and some pointless text pointers
    val stream = new NamedEntityEnrichment(EntityEnrichment.configureStream, TwitterConfig.TARGET_KAFKA_TOPIC, TwitterConfig.ENRICHED_KAFKA_TOPIC)
    stream.stream()

    // start the twitter orchestration worker
    val produceFuture = twitterProducer.orchestrate()

    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(longWaitTime)

    // tell the worker that it is time to stop listening for new tweets
    twitterProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    fakeConsumer.close()
  }
}
