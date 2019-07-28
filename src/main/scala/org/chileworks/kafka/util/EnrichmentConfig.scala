package org.chileworks.kafka.util

object EnrichmentConfig {
  val RAW_TOPIC = "topic4"
  val RICH_TOPIC = "topic5"
  val STREAM_APP = "named_entity_enrichment"
  val SPOTLIGHT_URL = "http://192.168.1.188:2222/rest/annotate"
  val SPOTLIGHT_CONFIDENCE_MIN = 0.7f
  val SPOTLIGHT_SUPPORT_MIN = 20
  val SIMILARITY_MIN = 0.7d
  val SECOND_RANK_DISTANCE_MAX = 0.1d
  val NORM_EDIT_DISTANCE_MIN = 0.7d
  def WIKI_URL_PREFIX(lang: String) = s"https://$lang.wikipedia.org/wiki/"
}
