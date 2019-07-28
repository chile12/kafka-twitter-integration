package org.chileworks.kafka.enrichment

import org.chileworks.kafka.TweetGeneratorUtil._
import org.chileworks.kafka.consumer.{SimpleConsumer, TweetConsumer}
import org.chileworks.kafka.producers.{FakeTweetProducer, TwitterFeedProducer}
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class NamedEntityEnrichmentTest extends FunSuite {

  private val fakeUser1 = createUser("Markus Freude", "Chile", "Leeptsch")
  private val fakeUser2 = createUser("Jan Vorberger", "HolyCrab", "Leeptsch")

  test("round-trip fake tweets"){

    val fakeProducer = new FakeTweetProducer("fake", TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"))

    val fakeTweets = Seq(
      createTweet(fakeUser2, "Hamburg is a marvelous city!", "en"),
      createTweet(fakeUser1, "Dude, I can only say Hamburg harbour under the skyline of the Elbphilharmonie.", "en"),
      createTweet(fakeUser2, "I will be there tomorrow, now I will have a look at the Miniatur Wunderland.", "en")
    )

    // creating a fake enrichment factory: sentiment is random, and some pointless text pointers
    val stream = new NamedEntityEnrichment(EntityEnrichment.configureStream)
    stream.stream()

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
    val res = Await.result(consumeFuture, defaultWaitTime)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    fakeConsumer.close()
    stream.close()
  }

}
