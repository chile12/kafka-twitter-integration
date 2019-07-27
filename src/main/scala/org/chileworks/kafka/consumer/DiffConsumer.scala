package org.chileworks.kafka.consumer

import java.time.Duration
import java.util.Properties

import org.chileworks.kafka.model.{RichTweet, Tweet}
import org.chileworks.kafka.util.EnrichmentConfig

import scala.collection.mutable

class DiffConsumer(val properties: Properties, originalTopic: String, enrichedTopic: String) extends TweetConsumer[Tweet] {

  // stores tweets with no matching enriched sibling between two polls
  private val danglingTweets = new mutable.HashMap[Long, Tweet]()

  override val topics: List[String] = List(EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC)
  override val maxQueueSize: Int = 1000000

  private def getDIff(prev: Tweet, next: Tweet): Option[String] ={  //TODO should be another object
    prev match {
      case rt: RichTweet => Option(prev.text.diff(next.text))
      case t: Tweet if prev.isInstanceOf[RichTweet] => Option(next.text.diff(prev.text))
      case _ => throw new IllegalArgumentException("Found sibling tweets without enrichment!")
    }

  }

  def pollDiffs(duration: Duration): Iterable[String] = {
    val allTweets = collect(1000).toList.groupBy(_.id)
    allTweets.flatMap {
      case (id, prev :: next :: Nil) => getDIff(prev, next)
      case (id, single :: Nil) => {
        danglingTweets.get(single.id) match {
          case Some(prev: Tweet) => getDIff(single, prev)
          case None =>
            danglingTweets.put(single.id, single)
            None
        }
      }
      case _ => None
    }
    Seq()
  }

  override def consumeTweet(topic: Long, tweet: Tweet): Tweet = tweet
}
