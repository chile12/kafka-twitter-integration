package org.chileworks.kafka.producers

import java.util.Properties

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer._
import org.chileworks.kafka.util.TwitterConfig
import scala.collection.JavaConverters._

/**
  * Instances of this class can copy one or more Twitter hashtag feeds into a Kafka topic stream.
  * @param id - name of the producer
  * @param properties - Properties
  */
class FeedTweetProducer(val id: String, val topics: List[String], properties: Properties) extends KafkaProducer[Long, String](properties) with TwitterFeedProducer {

  // Configure oauth
  private val authentication = new OAuth1(
    TwitterConfig.CONSUMER_KEY,
    TwitterConfig.CONSUMER_SECRET,
    TwitterConfig.ACCESS_TOKEN,
    TwitterConfig.TOKEN_SECRET
  )
  // track the terms of your choice. here im only tracking #bigdata.
  private val endpoint = new StatusesFilterEndpoint
  endpoint.trackTerms(TwitterConfig.HASHTAGS.split(",").map(_.trim).toList.asJava)

  // Twitter client configured by TwitterConfig, automatically filling up queue with received tweets
  private val client: BasicClient = new ClientBuilder()
    .hosts(Constants.STREAM_HOST)
    .authentication(authentication)
    .endpoint(endpoint)
    .processor(tweetProcessor)  // note here the processor is forwarded directly, no need to call TweetProducer::publishTweet
    .build

  override def beforeRun(): Unit = client.connect()

  override def afterRun(): Unit = {
    client.stop()
    this.close()
  }
}
