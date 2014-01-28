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
class TestCorpora extends FlatSpec with ShouldMatchers {

  val corpusPath: String = "/home/alexandre/projects/zaknarfen/dbpedia-spotlight/eval/src/test/scala/org/dbpedia/spotlight/util/CandidateMapEvalTest_mocks/mock-MilneWitten"
  val candidateMapPath: String = "/home/alexandre/projects/zaknarfen/dbpedia-spotlight/eval/src/test/scala/org/dbpedia/spotlight/util/CandidateMapEvalTest_mocks/mock-candidateMap.tsv"

  "The candidate map evaluation" should "match the metrics for the mock corpus" in {
    //Load Gold Standard
    val goldStandard: GoldStandardEssentials = GoldStandardEssentials.buildFromMilneWittenCorpus(new File(corpusPath))
    //Load Candidate Map
    val candidateMap: CandidateMapEssentials = new CandidateMapEssentials(candidateMapPath)

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
    val candidateMap: CandidateMapEssentials = new CandidateMapEssentials(candidateMapPath)

    val eval = new CandidateMapEval(goldStandard, candidateMap)

    eval.surfaceFormsRecall should be === -1
    eval.resourcesRecall should be === -1
  }


}
