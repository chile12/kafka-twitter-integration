package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonDeserializationContext, JsonDeserializer, JsonElement}

import scala.collection.JavaConverters._

case class SpotlightWrapper(
   text: String,
   confidence: Float,
   support: Int,
   types: Seq[String],
   sparql: String,
   policy: String,
   resources: Seq[SpotlightAnnotation]
 )

object SpotlightWrapper extends JsonDeserializer[SpotlightWrapper]{
  implicit val typeOfSrc: Type = new TypeToken[SpotlightWrapper](){}.getType

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): SpotlightWrapper = {
    val obj = json.getAsJsonObject
    SpotlightWrapper(
      obj.get("@text").getAsString,
      obj.get("@confidence").getAsFloat,
      obj.get("@support").getAsInt,
      obj.get("@types").getAsString.split(",").map(_.trim),
      obj.get("@sparql").getAsString,
      obj.get("@policy").getAsString,
      obj.getAsJsonArray("Resources").iterator().asScala.toSeq.map(o =>{
        SpotlightAnnotation.deserialize(o, SpotlightAnnotation.typeOfSrc, context)
      })
    )
  }
}
