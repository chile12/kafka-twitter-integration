package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson._
import com.google.gson.reflect.TypeToken

import scala.collection.JavaConverters._

case class EntitiesObj(hashtags: Seq[Hashtag], urls: Seq[UrlObj])

object EntitiesObj extends JsonSerializer[EntitiesObj] with JsonDeserializer[EntitiesObj] {

  implicit val typeOfSrc: Type = new TypeToken[EntitiesObj]() {}.getType

  override def serialize(src: EntitiesObj, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val t = new JsonObject
    t.add("hashtags", {
      val a = new JsonArray
      src.hashtags.foreach(h => a.add(context.serialize(h)))
      a
    })
    t.add("urls", {
      val a = new JsonArray
      src.urls.foreach(h => a.add(context.serialize(h)))
      a
    })
    t
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): EntitiesObj = {
    val obj = json.getAsJsonObject
    EntitiesObj(
      obj.getAsJsonArray("hashtags").iterator().asScala.map(je => context.deserialize(je, Hashtag.typeOfSrc).asInstanceOf[Hashtag]).toSeq,
      obj.getAsJsonArray("urls").iterator().asScala.map(je => context.deserialize(je, UrlObj.typeOfSrc).asInstanceOf[UrlObj]).toSeq
    )
  }
}
