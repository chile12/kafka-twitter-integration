package org.chileworks.kafka.consumer

import org.chileworks.kafka.TweetGeneratorUtil._
import org.chileworks.kafka.enrichment.{EntityEnrichment, NamedEntityEnrichment}
import org.chileworks.kafka.producers.{FakeTweetProducer, TwitterFeedProducer}
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}
import org.scalatest.{FunSuite, MustMatchers}

import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class AnnotationDisplayConsumerTest extends FunSuite with MustMatchers {

  private val fakeUser1 = createUser("Markus Freude", "Chile", "Leeptsch")
  private val fakeUser2 = createUser("Jan Vorberger", "HolyCrab", "Leeptsch")

  test("consume given tweets and produce an annotation-summary"){

    val fakeProducer = new FakeTweetProducer("fake", List(EnrichmentConfig.RAW_TOPIC), TwitterFeedProducer.configureProducer)
    val fakeConsumer = new AnnotationDisplayConsumer(TweetConsumer.configureConsumer("annotation_display_consumer"), EnrichmentConfig.RICH_TOPIC)
    fakeConsumer.start()

    val fakeTweets = Seq(
      createTweet(fakeUser2, "Hamburg is a marvelous city!", "en"),
      createTweet(fakeUser1, "Dude, I can only say Hamburg harbour under the skyline of the Elbphilharmonie.", "en"),
      createTweet(fakeUser2, "I will be there tomorrow, now I will have a look on the Alster.", "en")
    )

    // creating a fake enrichment factory: sentiment is random, and some pointless text pointers
    val stream = new NamedEntityEnrichment(EntityEnrichment.configureStream, EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC)
    stream.stream()

    // start the twitter orchestration worker
    val produceFuture = fakeProducer.orchestrate()
    // queue fake tweets
    fakeTweets.foreach(fakeProducer.publishTweet)
    // wait for the tweets to be processes
    TwitterFeedProducer.waitFor(defaultWaitTime)
    val consumeFuture = collectTweets(fakeConsumer, consumeTime).andThen{
      // here we test if the consumed tweets are all in the list of tweets we just send
      case Success(text) => text
      case Failure(f) => throw new IllegalStateException("No tweets could be collected.", f)
    }
    val res = Await.result(consumeFuture, defaultWaitTime)
    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    fakeConsumer.close()

    System.out.println(res.mkString("\n"))
  }

}
