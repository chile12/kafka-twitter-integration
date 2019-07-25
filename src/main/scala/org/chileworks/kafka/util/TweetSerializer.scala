package org.chileworks.kafka.util

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import java.lang.reflect.Type

import org.chileworks.kafka.model.Tweet

class TweetSerializer extends JsonSerializer[Tweet] {
  def serialize(tweet: Tweet, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val jsonObject = new JsonObject

    //TODO
    jsonObject.addProperty("id", tweet.id)

    jsonObject
  }
}
