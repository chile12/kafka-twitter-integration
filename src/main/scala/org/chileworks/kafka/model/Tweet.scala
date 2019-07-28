package org.chileworks.kafka.model

import java.lang.reflect.Type
import java.util.{Calendar, GregorianCalendar}

import com.google.gson._
import com.google.gson.reflect.TypeToken

/**
  * The tweet object as defined by twitter: https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
  * All attributes and further twitter objects are also explained there
  * NOTE: Minus attributes which are of no importance in this demo (ergo the enriched tweets will miss some attributes)
  *
  */
case class Tweet(
  id: Long,
  text: String,
  lang: String,
  user: User,
  retweetCount: Int,
  favoriteCount: Int,
  timestamp: Long,
  entitiesObj: Option[EntitiesObj] = None
){
  assert(text.trim.length <= 280, "Tweets may only contain up to 280 characters.")

  def getDateTime: String = Tweet.toXmlDateTime(timestamp)
}

object Tweet extends JsonSerializer[Tweet] with JsonDeserializer[Tweet]{

  def apply(tweet: Tweet, entObj: EntitiesObj): Tweet =
    Tweet(tweet.id, tweet.text, tweet.lang, tweet.user, tweet.retweetCount, tweet.favoriteCount, tweet.timestamp, Some(entObj))

  implicit val typeOfSrc: Type = new TypeToken[Tweet](){}.getType

  private def pad(sb: StringBuilder, x: Int): Unit = {
    if (x < 10) sb.append('0')
    sb.append(x)
  }

  def toXmlDateTime(millis: Long): String = {
    val sb = new StringBuilder
    val cal = new GregorianCalendar
    cal.setTimeInMillis(millis)
    sb.append(cal.get(Calendar.YEAR))
    sb.append('-')
    pad(sb, cal.get(Calendar.MONTH) + 1)
    sb.append('-')
    pad(sb, cal.get(Calendar.DAY_OF_MONTH))
    sb.append('T')
    pad(sb, cal.get(Calendar.HOUR_OF_DAY))
    sb.append(':')
    pad(sb, cal.get(Calendar.MINUTE))
    sb.append(':')
    pad(sb, cal.get(Calendar.SECOND))
    sb.toString()
  }

  override def serialize(src: Tweet, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {

    val t = new JsonObject
    t.addProperty("typ", "tweet")
    t.addProperty("id", src.id)
    t.addProperty("text", src.text)
    t.addProperty("lang", src.lang)
    t.add("user", User.serialize(src.user, User.typeOfSrc, context))
    t.addProperty("retweet_count", src.retweetCount)
    t.addProperty("timestamp_ms", src.timestamp)
    t.addProperty("favorite_count", src.favoriteCount)
    if(src.entitiesObj.nonEmpty) t.add("entities", context.serialize(src.entitiesObj.get))
    t
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Tweet = {
    val obj = json.getAsJsonObject
    Tweet(
      obj.get("id").getAsLong,
      obj.get("text").getAsString,
      obj.get("lang").getAsString,
      context.deserialize(obj.getAsJsonObject("user"), User.typeOfSrc),
      obj.get("retweet_count").getAsInt,
      obj.get("favorite_count").getAsInt,
      obj.get("timestamp_ms").getAsLong,
      Option(obj.get("entities")).map(o => context.deserialize(o.getAsJsonObject, EntitiesObj.typeOfSrc).asInstanceOf[EntitiesObj])
    )
  }
}
