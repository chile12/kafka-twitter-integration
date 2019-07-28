package org.chileworks.kafka.consumer

import java.time.Duration
import java.util.Properties

import org.chileworks.kafka.model.{EntitiesObj, Tweet}
import org.chileworks.kafka.util.EnrichmentConfig

import scala.collection.mutable

class DiffConsumer(val properties: Properties, originalTopic: String, enrichedTopic: String) extends TweetConsumer[Tweet] {

  // stores tweets with no matching enriched sibling between two polls
  private val danglingTweets = new mutable.HashMap[Long, Tweet]()

  override val topics: List[String] = List(EnrichmentConfig.RAW_TOPIC, EnrichmentConfig.RICH_TOPIC)
  override val maxQueueSize: Int = 1000000

  //TODO

  def pollDiffs(duration: Duration): Iterable[EntitiesObj] = {
    val allTweets = collect(1000).toList.groupBy(_.id)
    allTweets.flatMap {
      case (id, prev :: next :: Nil) => Some(prev.entitiesObj.getOrElse(next.entitiesObj.get))
      case (id, single :: Nil) => single.entitiesObj match {
        case Some(e) => Some(e)
        case None => danglingTweets.get(single.id) match {
          case Some(prev: Tweet) => prev.entitiesObj
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
