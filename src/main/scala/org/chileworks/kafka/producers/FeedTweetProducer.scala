package org.chileworks.kafka.producers

import java.util.Collections
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.chileworks.kafka.util.TwitterConfig
import org.apache.kafka.clients.producer._


class FeedTweetProducer(val id: String) extends KafkaProducer[Long, String](TwitterFeedProducer.configureProducer) with TwitterFeedProducer {

  // Configure auth
  private val authentication = new OAuth1(TwitterConfig.CONSUMER_KEY, TwitterConfig.CONSUMER_SECRET, TwitterConfig.ACCESS_TOKEN, TwitterConfig.TOKEN_SECRET)
  // track the terms of your choice. here im only tracking #bigdata.
  private val endpoint = new StatusesFilterEndpoint
  endpoint.trackTerms(Collections.singletonList(TwitterConfig.HASHTAG))
  override val queueHandle: BlockingQueue[String] = new LinkedBlockingQueue[String](TwitterFeedProducer.DEFAULTQUEUESIZE)

  // Twitter client configured by TwitterConfig, automatically filling up queue with received tweets
  private val client: BasicClient = new ClientBuilder()
    .hosts(Constants.STREAM_HOST)
    .authentication(authentication)
    .endpoint(endpoint)
    .processor(new StringDelimitedProcessor(queueHandle))
    .build

  override def beforeRun(): Unit = client.connect()

  override def afterRun(): Unit = {
    client.stop()
    this.close()
  }
}