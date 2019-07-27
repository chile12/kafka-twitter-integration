package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson._

import scala.collection.JavaConverters._

case class Enrichment(entities: Map[Int, String], sentiment: Float)  // TODO switch from String to other object

object Enrichment extends JsonSerializer[Enrichment] with JsonDeserializer[Enrichment]{

  implicit val typeOfSrc: Type = new TypeToken[Enrichment](){}.getType

  override def serialize(src: Enrichment, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val map = new JsonObject
    src.entities.foreach(kv => map.addProperty(String.valueOf(kv._1), kv._2))
    val e = new JsonObject
    e.add("entities", map)
    e.addProperty("sentiment", src.sentiment)
    e
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Enrichment = {
    val obj = json.getAsJsonObject
    val map = obj.getAsJsonObject("entities")
    new Enrichment(
      map.entrySet().asScala.map(kv => kv.getKey.toInt -> kv.getValue.getAsString).toMap,
      obj.get("sentiment").getAsFloat
    )
  }
}
