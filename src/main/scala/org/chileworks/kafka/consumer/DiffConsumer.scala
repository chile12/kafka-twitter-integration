package org.chileworks.kafka.consumer

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.chileworks.kafka.model.{RichTweet, Tweet}

import scala.collection.mutable

class DiffConsumer(properties: Properties, originalTopic: String, enrichedTopic: String) extends SimpleConsumer(new Properties()) {

  // stores tweets with no matching enriched sibling between two polls
  private val danglingTweets = new mutable.HashMap[Long, Tweet]()

  // subscribe to exactly those two topics
  super.subscribe(util.Arrays.asList(originalTopic, enrichedTopic))

  private def getDIff(prev: Tweet, next: Tweet): Option[String] ={  //TODO should be another object
    prev match {
      case rt: RichTweet => Option(prev.text.diff(next.text))
      case t: Tweet if prev.isInstanceOf[RichTweet] => Option(next.text.diff(prev.text))
      case _ => throw new IllegalArgumentException("Found sibling tweets without enrichment!")
    }

  }

  //TODO should return an
  def pollDiffs(duration: Duration): Iterable[String] = {
    val allTweets = pollTweets(duration).toList.groupBy(_.id)
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
  }

  // blunting all subscribe methods to prevent the subscription of other channels
  override def subscribe(topics: util.Collection[String], listener: ConsumerRebalanceListener): Unit = Unit

  override def subscribe(topics: util.Collection[String]): Unit = Unit

  override def subscribe(pattern: Pattern, listener: ConsumerRebalanceListener): Unit = Unit

  override def subscribe(pattern: Pattern): Unit = Unit

  override def unsubscribe(): Unit = Unit
}
