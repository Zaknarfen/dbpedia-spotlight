package org.dbpedia.spotlight.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.io.Source
import java.io.{PrintStream, File}
import org.dbpedia.spotlight.log.SpotlightLog

/**
 * Unit test for the ExtractCandidateMap class. Verify if the extracted map is valid.
 *
 * @author Alexandre Cançado Cardoso - accardoso
 */

@RunWith(classOf[JUnitRunner])
class ExtractCandidateMapTest extends FlatSpec with ShouldMatchers {

  ExtractCandidateMapTest.setConfigFile("conf/indexing.properties")

  ExtractCandidateMapTest.runExtractCandidateMapFrom("TRD")
  ExtractCandidateMapTest.runExtractCandidateMapFrom("TRDOM")


  "Every candidate at the extracted candidate map from any source" should "have the all fields (uri, surface form, count, source) valid" in {
    ExtractCandidateMapTest.finalCandidateMapsFilesNames.foreach{ map =>
      Source.fromFile(map._2).getLines.foreach{ candidate =>
        val candidateArray: Array[String] = candidate.split('\t')
        candidateArray.length should be === 4

        //Verifica se a uri é valida (não vazia)
        candidateArray(0) should not be ""
        //Verifica se a surface form é valida (não vazia)
        candidateArray(1) should not be ""
        //Verifica se o count é valido (inteiro maior ou igual a 1)
        candidateArray(2).toInt should be >= 1
        //Verifica se a cada uma da source é valida (está contida na souces utilizadas para criar o mapa )
        candidateArray(3).toList.foreach{ candidateSource =>
          map._1.contains(candidateSource.toString) should be === true
        }
      }
    }
  }

  "The temporary files" should "be removed" in {
    ExtractCandidateMapTest.cleanTempFiles() should be === true
  }

}

object ExtractCandidateMapTest {
  private var config: IndexingConfiguration = null
  private var configFileNameTuple: (String, String) = null //( fileName , fileExtension )

  private var titlesConfigEntry: String = ""
  private var redirectsConfigEntry: String = ""
  private var disambiguationsConfigEntry: String = ""
  private var wikipediaDumpconfigEntry: String = ""
  private var conceptURIsConfigEntry: String = ""
  private var redirectTCConfigEntry: String= ""
  private var occsConfigEntry: String = ""
  private var surfaceFormsConfigEntry: String = ""
  private var language: String = ""
  private var languageConfigEntry: String = ""
  private var blacklistedURIPatternsConfigEntry: String = ""
  private var stopWordsConfigEntry: String = ""
  private var maximumSurfaceFormLengthConfigEntry: String = ""
  private var luceneAnalyzerConfigEntry: String = ""
  private var maxContextWindowSizeConfigEntry: String = ""
  private var minContextWindowSizeConfigEntry: String = ""
  private var languageIl8nCodeConfigEntry: String = ""

  private def setConfigFile(configFileName: String) = {
    config = new IndexingConfiguration(configFileName)

    val configFileNameExtensionIndex: Int = configFileName.lastIndexOf(".")
    configFileNameTuple = ( configFileName.substring(0, configFileNameExtensionIndex) , configFileName.substring(configFileNameExtensionIndex+1) )

    /* Get input configs */
    titlesConfigEntry          = config.get("org.dbpedia.spotlight.data.labels")
    redirectsConfigEntry       = config.get("org.dbpedia.spotlight.data.redirects")
    disambiguationsConfigEntry = config.get("org.dbpedia.spotlight.data.disambiguations")
    wikipediaDumpconfigEntry   = config.get("org.dbpedia.spotlight.data.wikipediaDump")
    /* Get output configs */
    conceptURIsConfigEntry     = config.get("org.dbpedia.spotlight.data.conceptURIs")
    redirectTCConfigEntry      = config.get("org.dbpedia.spotlight.data.redirectsTC")
    occsConfigEntry            = config.get("org.dbpedia.spotlight.data.occs")
    surfaceFormsConfigEntry    = config.get("org.dbpedia.spotlight.data.surfaceForms")
    /* Get execution options set at config file */
    language = config.getLanguage().toLowerCase
    languageConfigEntry = language
    languageIl8nCodeConfigEntry = config.get("org.dbpedia.spotlight.language_i18n_code")
    blacklistedURIPatternsConfigEntry = config.get("org.dbpedia.spotlight.data.badURIs."+language)
    stopWordsConfigEntry = config.get("org.dbpedia.spotlight.data.stopWords."+language)
    maximumSurfaceFormLengthConfigEntry = config.get("org.dbpedia.spotlight.data.maxSurfaceFormLength")
    luceneAnalyzerConfigEntry = config.get("org.dbpedia.spotlight.lucene.analyzer")
    maxContextWindowSizeConfigEntry = config.get("org.dbpedia.spotlight.data.maxContextWindowSize")
    minContextWindowSizeConfigEntry = config.get("org.dbpedia.spotlight.data.minContextWindowSize")
  }


  private var finalCandidateMapsFilesNames: Array[(String, String)] = Array() //Source's Codes ; map file path
  var tempFilesNames: Array[String] = Array()

  private def runExtractCandidateMapFrom(sourcesCodes: String) =
    ExtractCandidateMap.main(Array[String](buildTempConfigFile(sourcesCodes), sourcesCodes))

  /* Build a config file to the test (preventing overwrite of outputfiles) */
  private def buildTempConfigFile(sourcesCodes: String): String = {
    val newConfigFileName:String = configFileNameTuple._1+".ExtractCandidateMapTest_"+sourcesCodes+".tmp."+configFileNameTuple._2

    val stream = new PrintStream(newConfigFileName, "UTF-8")

    //Input and option lines, can use the same as the original config file
    stream.println("org.dbpedia.spotlight.data.labels = "+titlesConfigEntry)
    stream.println("org.dbpedia.spotlight.data.redirects = "+redirectsConfigEntry)
    stream.println("org.dbpedia.spotlight.data.disambiguations = "+disambiguationsConfigEntry)
    stream.println("org.dbpedia.spotlight.data.wikipediaDump = "+wikipediaDumpconfigEntry)
    stream.println("org.dbpedia.spotlight.language = "+languageConfigEntry)
    stream.println("org.dbpedia.spotlight.language_i18n_code = "+languageIl8nCodeConfigEntry)
    stream.println("org.dbpedia.spotlight.data.badURIs."+language+" = "+blacklistedURIPatternsConfigEntry)
    stream.println("org.dbpedia.spotlight.data.stopWords."+language+" = "+stopWordsConfigEntry)
    stream.println("org.dbpedia.spotlight.data.maxSurfaceFormLength = "+maximumSurfaceFormLengthConfigEntry)
    stream.println("org.dbpedia.spotlight.lucene.analyzer = "+luceneAnalyzerConfigEntry)
    stream.println("org.dbpedia.spotlight.data.maxContextWindowSize = "+maximumSurfaceFormLengthConfigEntry)
    stream.println("org.dbpedia.spotlight.data.minContextWindowSize = "+minContextWindowSizeConfigEntry)
    //Output lines, must use a different file to do not overwrite
    tempFilesNames = tempFilesNames :+ conceptURIsConfigEntry+".ExtractCandidateMapV2_"+sourcesCodes
    stream.println("org.dbpedia.spotlight.data.conceptURIs = "+tempFilesNames(tempFilesNames.length-1))
    tempFilesNames = tempFilesNames :+ conceptURIsConfigEntry+".ExtractCandidateMapV2_"+sourcesCodes+".NOT"
    tempFilesNames = tempFilesNames :+ redirectTCConfigEntry+".ExtractCandidateMapV2_"+sourcesCodes
    stream.println("org.dbpedia.spotlight.data.redirectsTC = "+tempFilesNames(tempFilesNames.length-1))
    tempFilesNames = tempFilesNames :+ occsConfigEntry+".ExtractCandidateMapV2_"+sourcesCodes
    stream.println("org.dbpedia.spotlight.data.occs = "+tempFilesNames(tempFilesNames.length-1))
    tempFilesNames = tempFilesNames :+ surfaceFormsConfigEntry+".ExtractCandidateMapV2_"+sourcesCodes
    stream.println("org.dbpedia.spotlight.data.surfaceForms = "+tempFilesNames(tempFilesNames.length-1))
    //The surfaceForms file is the final candidate maps output (will be used to validate this unit test)
    finalCandidateMapsFilesNames = finalCandidateMapsFilesNames :+ ( sourcesCodes , tempFilesNames(tempFilesNames.length-1) )

    stream.close()

    newConfigFileName
  }

  private def cleanTempFiles() : Boolean = {
    var success = true

    tempFilesNames.foreach{ fileName =>
      val file = new File(fileName)
      if(!file.delete() && file.exists()){
        SpotlightLog.warn(this.getClass, "Could not delete the temp file: %s", fileName)
        success = false
      }
    }
    finalCandidateMapsFilesNames.foreach{ tuple =>
      val file = new File(tuple._2)
      if(!file.delete() && file.exists()){
        SpotlightLog.warn(this.getClass, "Could not delete the temp file: %s", tuple._2)
        success = false
      }
    }

    success
  }

}