package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson._

import scala.collection.JavaConverters._

case class Enrichment(entities: Map[Int, UrlObj], sentiment: Float)

object Enrichment extends JsonSerializer[Enrichment] with JsonDeserializer[Enrichment]{

  implicit val typeOfSrc: Type = new TypeToken[Enrichment](){}.getType

  override def serialize(src: Enrichment, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val map = new JsonObject
    src.entities.foreach(kv => map.add(String.valueOf(kv._1),
      UrlObj.serialize(kv._2, UrlObj.typeOfSrc, context)))
    val e = new JsonObject
    e.add("entities", map)
    e.addProperty("sentiment", src.sentiment)
    e
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Enrichment = {
    val obj = json.getAsJsonObject
    val map = obj.getAsJsonObject("entities")
    new Enrichment(
      map.entrySet().asScala.map(kv => kv.getKey.toInt -> UrlObj.deserialize(kv.getValue, UrlObj.typeOfSrc, context)).toMap,
      obj.get("sentiment").getAsFloat
    )
  }
}
