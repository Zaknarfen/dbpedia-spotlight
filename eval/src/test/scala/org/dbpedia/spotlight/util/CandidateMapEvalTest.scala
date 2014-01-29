package org.dbpedia.spotlight.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import java.io.File
import org.dbpedia.spotlight.model.AnnotatedParagraph

/**
 * Unit test for CandidateMapEval class.
 *
 * @author Alexandre Cançado Cardoso - accardoso
 */

@RunWith(classOf[JUnitRunner])
class CandidateMapEvalTest extends FlatSpec with ShouldMatchers {

  //(M&W) Corpus mock
  val primaryCorpusPath: String = "src/test/scala/org/dbpedia/spotlight/util/CandidateMapEvalTest_mocks/mock-MilneWitten"
  val alternativeCorpusPath: String = "eval/"+primaryCorpusPath

  //Candidate Map (for sources TRDOM) mock
  val primaryCandidateMapPath: String = "src/test/scala/org/dbpedia/spotlight/util/CandidateMapEvalTest_mocks/mock-candidateMap.tsv"
  val alternativeCandidateMapPath: String = "eval/"+primaryCandidateMapPath

  //The input's path can variate depending of the working directory
  var corpusDir: File = new File(primaryCorpusPath)
  if(!corpusDir.isDirectory){
    corpusDir = new File(alternativeCorpusPath)
  }
  var candidateMapFile: File = new File(primaryCandidateMapPath)
  if(!candidateMapFile.isFile)
    candidateMapFile = new File(alternativeCandidateMapPath)


  /* Tests */

  "The candidate map evaluation" should "match the metrics for the mock corpus" in {
    //Load Gold Standard
    val goldStandard: GoldStandardEssentials = GoldStandardEssentials.buildFromMilneWittenCorpus(corpusDir)
    //Load Candidate Map
    val candidateMap: CandidateMapEssentials = new CandidateMapEssentials(candidateMapFile)

    val eval = new CandidateMapEval(goldStandard, candidateMap)

    eval.getNumOfGoldStandardsAnnotations should be === 31
    eval.getNumOfGoldStandardsSfWithoutDuplication should be === 31
    eval.getNumOfCandidateMapSurfaceForms should be === 28

    eval.getNumOfGoldStandardsSfAtCandidateMap should be === 11
    eval.getNumOfGoldStandardsURIsAtRespectiveCandidateURIs should be === 10
    eval.getNumOfGoldStandardsURIsNotAtRespectiveCandidateURIs  should be (eval.getNumOfGoldStandardsSfAtCandidateMap - eval.getNumOfGoldStandardsURIsAtRespectiveCandidateURIs)
    eval.getNumOfGoldStandardsSfNotAtCandidateMap should be === 20

    eval.surfaceFormsRecall should be === (eval.getNumOfGoldStandardsSfAtCandidateMap.toDouble / eval.getNumOfGoldStandardsSfWithoutDuplication.toDouble)
    eval.resourcesRecall should be === (eval.getNumOfGoldStandardsURIsAtRespectiveCandidateURIs.toDouble / eval.getNumOfGoldStandardsSfAtCandidateMap.toDouble)
  }

  it should "have invalid recalls" in {
    val goldStandard: GoldStandardEssentials = new GoldStandardEssentials(List[AnnotatedParagraph]())
    val candidateMap: CandidateMapEssentials = new CandidateMapEssentials(candidateMapFile)

    val eval = new CandidateMapEval(goldStandard, candidateMap)

    eval.surfaceFormsRecall should be === -1
    eval.resourcesRecall should be === -1
  }


}
