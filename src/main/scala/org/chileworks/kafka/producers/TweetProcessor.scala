package org.chileworks.kafka.producers

import java.io.{IOException, InputStream}
import java.util.concurrent.BlockingQueue

import com.google.gson.{Gson, GsonBuilder, JsonParseException}
import com.twitter.hbc.common.DelimitedStreamReader
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.processor.AbstractProcessor
import org.chileworks.kafka.model.{Enrichment, RichTweet, Tweet, User}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Will continuously process each line of an input stream and tries to create a Tweet out of each.
  * User for parsing json tweets from dump or other sources.
  * @param queue - the BlockingQueue to which each Tweet is saved
  */
class TweetProcessor(queue: BlockingQueue[Tweet]) extends AbstractProcessor[Tweet](queue){
  private val logger = LoggerFactory.getLogger(classOf[TweetProcessor])
  private val MAX_ALLOWABLE_BUFFER_SIZE = 500000
  private val DEFAULT_BUFFER_SIZE = 50000
  private val EMPTY_LINE = ""
  private val gson: Gson = TweetProcessor.getTweetGson
  private var reader: DelimitedStreamReader = _

  /**
    * create a new Tweet based on the next line of the stream
    */
  override def processNextMessage(): Tweet = {
    var delimitedCount = -1
    var retries = 0
    while ( {
      delimitedCount < 0 && retries < 3
    }) {
      val line = reader.readLine
      if (line == null) throw new IOException("Unable to read new line from stream")
      else if (line == EMPTY_LINE) return null
      try
        delimitedCount = line.toInt
      catch {
        case n: NumberFormatException =>
          // resilience against the occasional malformed message
          logger.warn("Error parsing delimited length", n)
      }
      retries += 1
    }

    if (delimitedCount < 0) throw new RuntimeException("Unable to process delimited length")

    if (delimitedCount > MAX_ALLOWABLE_BUFFER_SIZE) { // this is to protect us from nastiness
      throw new IOException("Unreasonable message size " + delimitedCount)
    }
    val rawJson = reader.read(delimitedCount)
    Try{gson.fromJson(rawJson, classOf[Tweet])} match{
      case Success(tweet) => tweet
      case Failure(f) => throw new JsonParseException("A received tweet did not match the expected json syntax", f)
    }
  }

  override def setup(input: InputStream): Unit = {
    reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET, DEFAULT_BUFFER_SIZE)
  }
}

object TweetProcessor{
  def getTweetGson: Gson = new GsonBuilder()
    .registerTypeAdapter(classOf[Tweet], Tweet)
    .registerTypeAdapter(classOf[RichTweet], RichTweet)
    .registerTypeAdapter(classOf[User], User)
    .registerTypeAdapter(classOf[Enrichment], Enrichment)
    .create
}
