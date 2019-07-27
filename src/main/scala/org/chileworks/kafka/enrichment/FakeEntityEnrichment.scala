package org.chileworks.kafka.enrichment

import java.util.Properties

import org.chileworks.kafka.model.{Enrichment, RichTweet, Tweet}

class FakeEntityEnrichment(
    appendWith: String,
    sentimentFunc: Tweet => Float,
    additional: Map[Int, String] = Map()
  ) extends EntityEnrichment {

  override val properties: Properties = EntityEnrichment.configureStream

  override def enrichTweet(tweet: Tweet): RichTweet = {
    val sentiment = sentimentFunc(tweet)
    assert(sentiment >= 0 && sentiment <= 1)
    val enrichment = Enrichment(additional, sentiment)
    RichTweet(tweet.copy(text = tweet.text + appendWith), enrichment)
  }
}
