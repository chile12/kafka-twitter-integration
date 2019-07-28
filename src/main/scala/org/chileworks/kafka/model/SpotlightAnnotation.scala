package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonDeserializationContext, JsonDeserializer, JsonElement}


/**
  * A Spotlight annotation.
  */
case class SpotlightAnnotation(
 uri: String,
 support: Int,
 types: Seq[String],
 surfaceForm: String,
 offset: Int,
 similarityScore: Double,
 percentageOfSecondRank: Double
)

object SpotlightAnnotation extends JsonDeserializer[SpotlightAnnotation]{
  implicit val typeOfSrc: Type = new TypeToken[SpotlightAnnotation](){}.getType

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): SpotlightAnnotation = {
    val obj = json.getAsJsonObject
    SpotlightAnnotation(
      obj.get("@URI").getAsString,
      obj.get("@support").getAsInt,
      obj.get("@types").getAsString.split(",").map(_.trim),
      obj.get("@surfaceForm").getAsString,
      obj.get("@offset").getAsInt,
      obj.get("@similarityScore").getAsDouble,
      obj.get("@percentageOfSecondRank").getAsDouble
    )
  }
}
