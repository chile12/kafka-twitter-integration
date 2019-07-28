package org.chileworks.kafka.enrichment

import java.util.Properties

import com.sun.xml.internal.bind.v2.util.EditDistance
import org.apache.http.client.HttpResponseException
import org.chileworks.kafka.model
import org.chileworks.kafka.model._
import org.chileworks.kafka.producers.TweetProcessor
import org.chileworks.kafka.util.EnrichmentConfig._
import scalaj.http.Http

import scala.util.{Failure, Success, Try}

/**
  * The NER based Enrichment of Tweets, will query the tweet text for known entities and
  * retrieve additional data about these entities and enrich the tweet with it.
  */
class NamedEntityEnrichment(val properties: Properties, override val rawTopic: String, override val richTopic: String) extends EntityEnrichment {

  private val gson = TweetProcessor.getTweetGson
  private val typeOntologyPrefix = "Schema:"
  private val defaultLang = "en"

  /**
    * Will create an instance of [[Tweet]] based on the input [[Tweet]].
    *
    * @param tweet - the input Tweet
    * @return - the enriched Tweet
    */
  override def enrichTweet(tweet: Tweet): Tweet = {
    Tweet(tweet, extractEnrichmentFromTweet(tweet))
  }

  /**
    * Will connect to DBpedia spotlight and let it do NER to return additional info.
    * @param tweet - the tweet
    * @return - the Enrichment
    */
  private def extractEnrichmentFromTweet(tweet: Tweet): EntitiesObj ={
    querySpotlight(tweet.text) match{
      case Success(body) =>
        val wrapper = gson.fromJson(body, classOf[SpotlightWrapper])
        spotlightAnnotationsToEnrichment(wrapper)
      case Failure(f) => throw f  //TODO
    }
  }

  private def spotlightAnnotationsToEnrichment(wrapper: SpotlightWrapper): EntitiesObj ={
    val annotations = wrapper.resources.flatMap(anno => {
      // NOTE: since the label is not provided in the annotation object and I don't want a disambiguation
      // call to the API this is a close approximation of how to get the label from the uri, which is fine for our purpose
      val wikiPage = anno.uri.substring(anno.uri.lastIndexOf('/')+1)
      val label = wikiPage.replaceAll("_", " ")
      val normalizedLevenshtein = EditDistance.editDistance(label, anno.surfaceForm).toFloat / Math.max(label.length, anno.surfaceForm.length).toFloat

      if(anno.similarityScore < SIMILARITY_MIN) None
      else if(anno.percentageOfSecondRank > SECOND_RANK_DISTANCE_MAX) None
      else if(normalizedLevenshtein > NORM_EDIT_DISTANCE_MAX) None
      else{
        val wikiUrl = WIKI_URL_PREFIX(defaultLang) + wikiPage
        val entityType = anno.types.find(_.startsWith(typeOntologyPrefix)).map(t => t.diff(typeOntologyPrefix)).getOrElse("")
        Some(model.UrlObj(wikiUrl, wikiUrl, anno.surfaceForm, (anno.offset, anno.offset + anno.surfaceForm.length)))
      }
    })
    model.EntitiesObj(Seq(), annotations)
  }

  private def querySpotlight(text: String): Try[String] = {
    val http = Http(SPOTLIGHT_URL)
      .param("text", text)
      .param("confidence", String.valueOf(SPOTLIGHT_CONFIDENCE_MIN))
      .param("support", String.valueOf(SPOTLIGHT_SUPPORT_MIN))
      .header("Accept", "application/json")

    val jsonResponse = http.asString
    if(jsonResponse.isSuccess)
      Success(jsonResponse.body)
    else
      Failure(new HttpResponseException(jsonResponse.code, jsonResponse.statusLine))
  }
}
