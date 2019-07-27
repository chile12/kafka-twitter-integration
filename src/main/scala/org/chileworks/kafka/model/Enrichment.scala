package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonElement, JsonObject, JsonSerializationContext, JsonSerializer}

case class Enrichment(entities: Map[Int, String], sentiment: Float)  // TODO switch from String to other object

object Enrichment extends JsonSerializer[Enrichment]{

  implicit val typeOfSrc: Type = new TypeToken[RichTweet](){}.getType

  override def serialize(src: Enrichment, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val map = new JsonObject
    src.entities.foreach(kv => map.addProperty(String.valueOf(kv._1), kv._2))
    val e = new JsonObject
    e.add("entities", map)
    e.addProperty("sentiment", src.sentiment)
    e
  }
}
