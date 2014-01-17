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

  ExtractCandidateMapTest.runExtractCandidateMap("TRD")
  ExtractCandidateMapTest.runExtractCandidateMap("TRDOM")

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
  //Test files (mock and temps) base directory
  val primaryTestFilesDirName: String = "src"+File.separator+"test"+File.separator+"scala"+File.separator+
                                        "org"+File.separator+"dbpedia"+File.separator+"spotlight"+File.separator+"util"+
                                        File.separator+"ExtractCandidateMapTest_mock"  //If working directory is the index module root
  val alternativeTestFilesDirName: String = "index"+File.separator+primaryTestFilesDirName //If working directory is the dbpedia-spotlight root

  var testFilesDir: File = new File(primaryTestFilesDirName)
  if(!testFilesDir.isDirectory)
    testFilesDir = new File(alternativeTestFilesDirName)

  val testFilesDirPath: String = testFilesDir.getCanonicalPath + File.separator

  //The config file to use the tests mocks 
  val testConfigFileName: String = "ExtractCandidateMapTest.indexing.tmp.properties"

  //Test inputs files
  val labelsFileName: String = "artificial-labels_pt.nt.bz2"
  val redirectsFileName: String = "artificial-redirects_pt.nt.bz2"
  val disambiguationsFileName: String = "artificial-disambiguations_pt.nt.bz2"
  val wikipediaDumpFileName: String = "artificial-ptwiki-latest-pages-articles.xml"

  var blacklistedURIPatternsFileName: String = "blacklistedURIPatterns.pt.list"
  val stopWordsFileName: String = "stopwords.list"
  
  //Test outputs files
  val conceptURIsFileName: String = "ExtractCandidateMapTest.conceptURIs.tmp.list"
  val redirectTCFileName: String= "ExtractCandidateMapTest.redirects_tc.tmp.tsv"
  val occsFileName: String = "ExtractCandidateMapTest.occs.tmp.tsv"
  val surfaceFormsFileName: String = "ExtractCandidateMapTest.surfaceForms.tmp.tsv"
  
  //Test extraction options
  val language: String = "Portuguese"
  val languageIl8nCode: String = "pt"
  var luceneAnalyzer: String = "org.apache.lucene.analysis.pt.PortugueseAnalyzer"
  var maximumSurfaceFormLength: String = "50"  
  var maxContextWindowSize: String = "200"
  var minContextWindowSize: String = "0"


  //List with all temporary files to run the tests
  var tempFilesNames: Array[String] = Array()

  //List with all extracted candidate map and its sources
  var finalCandidateMapsFilesNames: Array[(String, String)] = Array() //Source's Codes ; map file path
  
  def createTestConfigFile(sources: String): String = {
    val testConfigFilePath = testFilesDirPath+sources+"-"+testConfigFileName
    tempFilesNames = tempFilesNames :+ testConfigFilePath

    val stream = new PrintStream(testConfigFilePath, "UTF-8")

    //Input and option lines, can use the same as the original config file
    stream.println("org.dbpedia.spotlight.language = "+language)
    stream.println("org.dbpedia.spotlight.language_i18n_code = "+languageIl8nCode)
    stream.println("org.dbpedia.spotlight.data.maxSurfaceFormLength = "+maximumSurfaceFormLength)
    stream.println("org.dbpedia.spotlight.lucene.analyzer = "+luceneAnalyzer)
    stream.println("org.dbpedia.spotlight.data.maxContextWindowSize = "+maximumSurfaceFormLength)
    stream.println("org.dbpedia.spotlight.data.minContextWindowSize = "+minContextWindowSize)

    stream.println("org.dbpedia.spotlight.data.badURIs."+language.toLowerCase()+" = "+testFilesDirPath+blacklistedURIPatternsFileName)
    stream.println("org.dbpedia.spotlight.data.stopWords."+language.toLowerCase()+" = "+testFilesDirPath+stopWordsFileName)
    stream.println("org.dbpedia.spotlight.data.labels = "+testFilesDirPath+labelsFileName)
    stream.println("org.dbpedia.spotlight.data.redirects = "+testFilesDirPath+redirectsFileName)
    stream.println("org.dbpedia.spotlight.data.disambiguations = "+testFilesDirPath+disambiguationsFileName)
    stream.println("org.dbpedia.spotlight.data.wikipediaDump = "+testFilesDirPath+wikipediaDumpFileName)


    //Output lines, must use a different file to do not overwrite
    var sourceOutputTempFileName: String = testFilesDirPath+sources+"-"+conceptURIsFileName
    tempFilesNames = tempFilesNames :+ sourceOutputTempFileName
    tempFilesNames = tempFilesNames :+ (sourceOutputTempFileName+".NOT")
    stream.println("org.dbpedia.spotlight.data.conceptURIs = "+sourceOutputTempFileName)

    sourceOutputTempFileName = testFilesDirPath+sources+"-"+redirectTCFileName
    tempFilesNames = tempFilesNames :+ sourceOutputTempFileName
    stream.println("org.dbpedia.spotlight.data.redirectsTC = "+sourceOutputTempFileName)

    sourceOutputTempFileName = testFilesDirPath+sources+"-"+occsFileName
    tempFilesNames = tempFilesNames :+ sourceOutputTempFileName
    stream.println("org.dbpedia.spotlight.data.occs = "+sourceOutputTempFileName)

    sourceOutputTempFileName = testFilesDirPath+sources+"-"+surfaceFormsFileName
    tempFilesNames = tempFilesNames :+ sourceOutputTempFileName
    stream.println("org.dbpedia.spotlight.data.surfaceForms = "+sourceOutputTempFileName)
    finalCandidateMapsFilesNames = finalCandidateMapsFilesNames :+ (sources, sourceOutputTempFileName)

    stream.close()

    testConfigFilePath
  }

  def cleanTempFiles(): Boolean = {
    var couldNotDeleteFiles: List[String] = List()

    tempFilesNames.foreach{ tempFileName =>
      val tempFile = new File(tempFileName)
      if(!tempFile.delete() && tempFile.exists)
        couldNotDeleteFiles = couldNotDeleteFiles :+ tempFileName
    }

    if(!couldNotDeleteFiles.isEmpty){
      SpotlightLog.error(this.getClass, "Could not delete the temporary files listed bellow:\n%s",
                                        couldNotDeleteFiles.mkString("\n"))
      return false
    }

    true
  }



  def runExtractCandidateMap(sources: String) =
    ExtractCandidateMap.main(Array[String](ExtractCandidateMapTest.createTestConfigFile(sources), sources))
  
}