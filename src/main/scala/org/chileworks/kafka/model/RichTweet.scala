package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson._

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

object RichTweet extends JsonSerializer[RichTweet] with JsonDeserializer[RichTweet] {

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

  def determineTweetClass(rawTweet: String): Class[_ <: Tweet] = if(rawTweet.contains("\"rich-tweet\"")) classOf[RichTweet] else classOf[Tweet]

  override def serialize(src: RichTweet, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val rt = Tweet.serialize(src, Tweet.typeOfSrc, context).asInstanceOf[JsonObject]
    rt.remove("typ")
    rt.addProperty("typ", "rich-tweet")
    val enrichment = Enrichment.serialize(src.enrichment, Enrichment.typeOfSrc, context)
    rt.add("enrichment", enrichment)
    rt
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): RichTweet = {
    val obj = json.getAsJsonObject
    new RichTweet(
      obj.get("id").getAsLong,
      obj.get("text").getAsString,
      obj.get("lang").getAsString,
      context.deserialize(obj.getAsJsonObject("user"), User.typeOfSrc),
      obj.get("retweet_count").getAsInt,
      obj.get("favorite_count").getAsInt,
      obj.get("timestamp_ms").getAsLong,
      context.deserialize(obj.getAsJsonObject("enrichment"), Enrichment.typeOfSrc)
    )
  }
}
