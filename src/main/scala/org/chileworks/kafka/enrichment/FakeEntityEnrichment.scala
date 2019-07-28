package org.chileworks.kafka.enrichment

import java.util.Properties

import org.chileworks.kafka.model
import org.chileworks.kafka.model.{EntitiesObj, Hashtag, Tweet, UrlObj}

class FakeEntityEnrichment(
    appendWith: String,
    sentimentFunc: Tweet => Float,
    additional: Map[Int, UrlObj] = Map()
  ) extends EntityEnrichment[Tweet] {

  override val properties: Properties = EntityEnrichment.configureStream

  override def enrichTweet(tweet: Tweet): Tweet = {
    val sentiment = sentimentFunc(tweet)
    assert(sentiment >= 0 && sentiment <= 1)
    val enrichment = EntitiesObj(Seq(Hashtag((14, 21), "#bigdata")), additional.values.toSeq)
    model.Tweet(tweet.copy(text = tweet.text + appendWith), enrichment)
  }
}
