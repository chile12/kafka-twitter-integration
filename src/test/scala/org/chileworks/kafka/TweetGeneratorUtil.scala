package org.chileworks.kafka

import java.time
import java.util.Date
import java.util.concurrent.TimeUnit

import org.chileworks.kafka.consumer.TweetConsumer
import org.chileworks.kafka.model.{Tweet, User}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

object TweetGeneratorUtil {

  val defaultWaitTime: Duration = Duration.create(10d, TimeUnit.SECONDS)
  val longWaitTime: Duration = Duration.create(20d, TimeUnit.SECONDS)
  val consumeTime: time.Duration = time.Duration.ofSeconds(10L)

  private val idFactor = 1000000000000000000L
  private val followerFactor = 1000000L
  private val reTweetFactor = 10000L
  private val likeFactor = 100000L
  private val userIdFactor = 10000000L
  private val defaultCollectionSize = 1000

  private def random(factor: Long): Long = (Random.nextDouble() * factor).toLong

  def createUser(name: String, nick: String, location: String): User = User(random(userIdFactor), name, nick, location, random(followerFactor).toInt)

  def createTweet(user: User, text: String, lang: String): Tweet = Tweet(random(idFactor), text, lang, user, random(reTweetFactor).toInt, random(likeFactor).toInt, new Date().getTime)

  def collectTweets[T](consumer: TweetConsumer[T], duration: time.Duration): Future[Iterable[T]] = Future{
    val start = new Date().getTime
    var tweets: Iterable[T] = Iterable.empty
    var runningFor = time.Duration.ofMillis(0L)
    // let's give kafka some time
    Thread.sleep(100)
    while (tweets.isEmpty || runningFor.compareTo(duration) >= 0 ) {
      tweets = consumer.collect(defaultCollectionSize)
      runningFor = time.Duration.ofMillis(new Date().getTime - start)
    }
    tweets.toList
  }
}
