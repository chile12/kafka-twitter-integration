package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson._
import com.google.gson.reflect.TypeToken

case class Hashtag(indices: (Int, Int), text: String)

object Hashtag extends JsonSerializer[Hashtag] with JsonDeserializer[Hashtag]{

  implicit val typeOfSrc: Type = new TypeToken[Hashtag](){}.getType

  override def serialize(src: Hashtag, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val indices = new JsonArray(2)
    indices.add(src.indices._1)
    indices.add(src.indices._2)
    val t = new JsonObject
    t.add("indices", indices)
    t.addProperty("text", src.text)
    t
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Hashtag = {
    val obj = json.getAsJsonObject
    Hashtag(
      {
        val zw = obj.getAsJsonArray("indices")
        (zw.get(0).getAsInt, zw.get(1).getAsInt)
      },
      obj.get("text").getAsString
    )
  }
}
