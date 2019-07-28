package org.chileworks.kafka.enrichment

import org.chileworks.kafka.TweetGeneratorUtil._
import org.chileworks.kafka.consumer.{SimpleConsumer, TweetConsumer}
import org.chileworks.kafka.model.Tweet
import org.chileworks.kafka.producers.{FakeTweetProducer, TwitterFeedProducer}
import org.chileworks.kafka.util.{EnrichmentConfig, KafkaConfig}
import org.scalatest.{FunSuite, MustMatchers}

import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class NamedEntityEnrichmentTest extends FunSuite with MustMatchers {

  private val fakeUser1 = createUser("Markus Freude", "Chile", "Leeptsch")
  private val fakeUser2 = createUser("Jan Vorberger", "HolyCrab", "Leeptsch")

  test("round-trip fake tweets"){

    val fakeProducer = new FakeTweetProducer("fake", KafkaConfig.TOPICS.split(",").toList.map(_.trim), TwitterFeedProducer.configureProducer)
    val fakeConsumer = new SimpleConsumer(TweetConsumer.configureConsumer("simpler_consumer"), List(EnrichmentConfig.RICH_TOPIC))

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
      case Success(tweets) =>
        assert(tweets.nonEmpty)
        tweets
      case Failure(f) => throw new IllegalStateException("No tweets could be collected.", f)
    }
    val res = Await.result(consumeFuture, defaultWaitTime)

    testForUrlAnnotation(res, fakeTweets.head, Seq(("https://en.wikipedia.org/wiki/Hamburg", "Hamburg", (0,7))))
    testForUrlAnnotation(res, fakeTweets(1), Seq(
      ("https://en.wikipedia.org/wiki/Port_of_Hamburg", "Hamburg harbour", (21, 36)),
      ("https://en.wikipedia.org/wiki/Elbphilharmonie", "Elbphilharmonie", (62, 77))
    ))
    testForUrlAnnotation(res, fakeTweets(2), Seq(("https://en.wikipedia.org/wiki/Alster", "Alster", (56, 62))))

    // tell the worker that it is time to stop listening for new tweets
    fakeProducer.toggleListening()
    // wait for that to sink in and exit
    Await.ready(produceFuture, defaultWaitTime)
    fakeConsumer.close()
    stream.close()
  }

  private def testForUrlAnnotation(res: Iterable[Tweet], original: Tweet, urls: Seq[(String, String, (Int, Int))]): Unit = {
    res.find(t => t.id == original.id) match {
      case Some(rt) => rt.entitiesObj match {
        case Some(enrichment) =>
          enrichment.urls.size mustBe urls.size
          enrichment.urls.zip(urls).foreach[Unit]( tup =>{
            val (url, expected) = tup
            url.url mustBe expected._1
            url.surfaceForm mustBe expected._2
            url.indices mustBe expected._3
          })
        case None => fail()
      }
      case None => fail()
    }
  }
}
