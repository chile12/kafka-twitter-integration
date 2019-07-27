package org.chileworks.kafka.model

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonElement, JsonObject, JsonSerializationContext, JsonSerializer}

case class User(
   id: Long,
   name: String,
   screenName: String,
   location: String,
   followersCount: Int
 )

object User extends JsonSerializer[User]{
  implicit val typeOfSrc: Type = new TypeToken[User](){}.getType

  override def serialize(src: User, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val u = new JsonObject
    u.addProperty("id", src.id)
    u.addProperty("name", src.name)
    u.addProperty("screenName", src.screenName)
    u.addProperty("location", src.location)
    u.addProperty("followersCount", src.followersCount)
    u
  }
}
