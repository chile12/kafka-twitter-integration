package org.chileworks.kafka.model

import com.google.gson.annotations.SerializedName
import org.chileworks.kafka.model.User

case class Tweet(id: Long, text: String, lang: String, user: User, @SerializedName("retweet_count") retweetCount: Int, @SerializedName("favorite_count") favoriteCount: Int)
