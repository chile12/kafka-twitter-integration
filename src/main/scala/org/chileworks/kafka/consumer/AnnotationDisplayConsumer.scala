package org.chileworks.kafka.consumer

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.chileworks.kafka.model.{Tweet, UrlObj}

import scala.collection.mutable

class AnnotationDisplayConsumer(val properties: Properties, topic: String) extends TweetConsumer[String] {

  override val topics: List[String] = List(topic)
  override val maxQueueSize: Int = 1000000

  override def consumeTweet(topic: Long, tweet: Tweet): String = {
    tweet.entitiesObj match{
      case Some(enrichment) if enrichment.urls.nonEmpty =>
        val sb = new mutable.StringBuilder()
        sb.append("Annotations or tweet: ")
        sb.append(tweet.id)
        sb.append("\n")
        enrichment.urls.foreach{u: UrlObj =>
          sb.append("\t")
          sb.append(StringUtils.rightPad(u.surfaceForm, 25, " "))
          sb.append("at ")
          sb.append(u.indices._1)
          sb.append(":\t")
          sb.append(u.url)
          sb.append("\n")
        }
        sb.toString()
      case _ => ""
    }
  }
}
