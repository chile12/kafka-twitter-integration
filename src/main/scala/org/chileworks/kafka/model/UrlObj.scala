package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson._
import com.google.gson.reflect.TypeToken

case class UrlObj(url: String, expandedUrl: String, surfaceForm: String, indices: (Int, Int))

object UrlObj extends JsonSerializer[UrlObj] with JsonDeserializer[UrlObj]{

  implicit val typeOfSrc: Type = new TypeToken[UrlObj](){}.getType

  override def serialize(src: UrlObj, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val indices = new JsonArray(2)
    indices.add(src.indices._1)
    indices.add(src.indices._2)

    val u = new JsonObject
    u.addProperty("url", src.url)
    u.addProperty("expanded_url", src.expandedUrl)
    u.addProperty("display_url", src.surfaceForm)
    u.add("indices", indices)
    u
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): UrlObj = {
    val obj = json.getAsJsonObject
    UrlObj(
      obj.get("url").getAsString,
      obj.get("expanded_url").getAsString,
      obj.get("display_url").getAsString,
      {
        val zw = obj.getAsJsonArray("indices")
        (zw.get(0).getAsInt, zw.get(1).getAsInt)
      }
    )
  }
}
