package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonElement, JsonObject, JsonSerializationContext, JsonSerializer}

class RichTweet(
   id: Long,
   text: String,
   lang: String,
   user: User,
   retweetCount: Int,
   favoriteCount: Int,
   timestamp: Long,
   val enrichment: Enrichment
 ) extends Tweet(id, text, lang, user, retweetCount, favoriteCount, timestamp) {

}

object RichTweet extends JsonSerializer[RichTweet]{

  implicit val typeOfSrc: Type = new TypeToken[RichTweet](){}.getType

  def apply(
   id: Long,
   text: String,
   lang: String,
   user: User,
   retweetCount: Int,
   favoriteCount: Int,
   timestamp: Long,
   enrichment: Enrichment
  ): RichTweet = new RichTweet(id, text, lang, user, retweetCount, favoriteCount, timestamp, enrichment)

  def apply(t: Tweet, enrichment: Enrichment): RichTweet =
    apply(t.id, t.text, t.lang, t.user, t.retweetCount, t.favoriteCount, t.timestamp, enrichment)

  override def serialize(src: RichTweet, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val rt = Tweet.serialize(src, Tweet.typeOfSrc, context).asInstanceOf[JsonObject]
    val enrichment = Enrichment.serialize(src.enrichment, Enrichment.typeOfSrc, context)
    rt.add("enrichment", enrichment)
    rt
  }
}
