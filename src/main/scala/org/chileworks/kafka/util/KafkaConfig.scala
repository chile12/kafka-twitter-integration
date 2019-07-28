package org.chileworks.kafka.util

object KafkaConfig {

  val SERVERS = "192.168.1.188:9092"
  val TOPICS = "raw_twitter_feed"
  val SLEEP_TIMER = 1000
  val GROUP_ID = "default_group"
  val AUTO_OFFSET = "latest"   //or earliest
}
