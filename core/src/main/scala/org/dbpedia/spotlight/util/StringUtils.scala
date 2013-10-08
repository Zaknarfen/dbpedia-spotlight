package org.dbpedia.spotlight.util

import scala.util.matching.Regex

/**
 * Created with IntelliJ IDEA.
 * User: Renan
 * Date: 07/10/13
 * Time: 15:49
 * To change this template use File | Settings | File Templates.
 */
class StringUtils {
  def onlyUriEnding(wikiUrl: String): String = {
    //TODO: use WikipediaToDBpediaClosure.scala

    val pattern = new Regex("""(\w*)/resource/(\w*)""", "firstName", "lastName")
    var result = ""

    try {
      result = pattern.findFirstMatchIn(wikiUrl).get.group("lastName")
    } catch {
      case e: Exception => {
        //println("Warning! is this an URL? " + wikiUrl) // In case a string not containing a /wiki/ is passed
        wikiUrl
      }
    }
    result
  }
}
