package org.chileworks.kafka.model

import java.lang.reflect.Type
import java.util.{Calendar, GregorianCalendar}

import com.google.gson.{JsonElement, JsonObject, JsonSerializationContext, JsonSerializer}
import com.google.gson.reflect.TypeToken

case class Tweet(
  id: Long,
  text: String,
  lang: String,
  user: User,
  retweetCount: Int,
  favoriteCount: Int,
  timestamp: Long
){
  assert(text.trim.length <= 280, "Tweets may only contain up to 280 characters.")

  def getDateTime: String = Tweet.toXmlDateTime(timestamp)

  override def toString: String = Tweet.serialize(this, Tweet.typeOfSrc, null).toString
}

object Tweet extends JsonSerializer[Tweet]{

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
    t
  }
}
