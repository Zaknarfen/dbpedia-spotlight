package org.dbpedia.spotlight.util

import java.io.File
import org.dbpedia.spotlight.corpus.MilneWittenCorpus
import org.dbpedia.spotlight.model.{DBpediaResource, AnnotatedParagraph}
import scala.io.Source
import org.dbpedia.spotlight.log.SpotlightLog
import scala.util.control.Breaks._
import org.dbpedia.spotlight.model.SurfaceForm

/**
 * Evaluations class for the candidate maps extracted (by ExtractCandidateMap class) from different sources,
 * to define which sources combination is the best to use in Spotlight.
 *
 * @author Alexandre Cançado Cardoso - accardoso
 */

class CandidateMapEval(goldStandard: GoldStandardEssentials, candidateMap: CandidateMapEssentials){
  
  private var goldStandardsSfAtCandidateMap: List[SurfaceForm] = List()
  private var goldStandardsSfNotAtCandidateMap: List[SurfaceForm] = List()

  private var numOfGoldStandardsURIsAtRespectiveCandidateURIs: Int = 0
  private var numOfGoldStandardsURIsNotAtRespectiveCandidateURIs: Int = 0

  goldStandard.annotations.foreach{ annotation =>
    breakable{
      candidateMap.candidates.foreach{ candidate =>
        if(candidate.surfaceForm.equals(annotation.surfaceForm)){
          goldStandardsSfAtCandidateMap = goldStandardsSfAtCandidateMap :+ annotation.surfaceForm
          if(candidate.getResources.contains(annotation.resource)){
            numOfGoldStandardsURIsAtRespectiveCandidateURIs += 1
          }else{
            numOfGoldStandardsURIsNotAtRespectiveCandidateURIs += 1
          }
          break()
        }
      }
      //If not break, so there is no sf at CM for the sf of this GS annotation
      goldStandardsSfNotAtCandidateMap = goldStandardsSfNotAtCandidateMap :+ annotation.surfaceForm
    }
  }
  goldStandardsSfAtCandidateMap = goldStandardsSfAtCandidateMap.distinct
  goldStandardsSfNotAtCandidateMap = goldStandardsSfNotAtCandidateMap.distinct


  def getNumOfGoldStandardsSfAtCandidateMap: Int = goldStandardsSfAtCandidateMap.length
  def getNumOfGoldStandardsSfNotAtCandidateMap: Int = goldStandardsSfNotAtCandidateMap.length

  def getNumOfGoldStandardsURIsAtRespectiveCandidateURIs: Int = numOfGoldStandardsURIsAtRespectiveCandidateURIs
  def getNumOfGoldStandardsURIsNotAtRespectiveCandidateURIs: Int = numOfGoldStandardsURIsNotAtRespectiveCandidateURIs

  def getNumOfGoldStandardsAnnotations: Int = goldStandard.annotations.length //The GS set can have more than one annotations per sf
  def getNumOfGoldStandardsSfWithoutDuplication: Int = getNumOfGoldStandardsSfAtCandidateMap + getNumOfGoldStandardsSfNotAtCandidateMap //The GS set can have more than one annotations per sf, so here is number of the sf avoiding duplications
  def getNumOfCandidateMapSurfaceForms: Int = candidateMap.candidates.length //Each sf has only one candidate with multiples resources

  def surfaceFormsRecall: Double = {
    if(getNumOfGoldStandardsSfAtCandidateMap == 0)
      -1.0
    else
      getNumOfGoldStandardsSfAtCandidateMap.toDouble / getNumOfGoldStandardsSfWithoutDuplication.toDouble
  }

  def resourcesRecall: Double = {
    if(getNumOfGoldStandardsSfAtCandidateMap == 0)
      -1.0
    else
      getNumOfGoldStandardsURIsAtRespectiveCandidateURIs.toDouble / getNumOfGoldStandardsSfAtCandidateMap.toDouble
  }

}
object CandidateMapEval {

  def main(args: Array[String]){
    //Load Gold Standard
    val goldStandard: GoldStandardEssentials = GoldStandardEssentials.buildFromMilneWittenCorpus(new File(args(0)))
    //Load Candidate Map
    val candidateMap: CandidateMapEssentials = new CandidateMapEssentials(new File(args(1)))

    val eval = new CandidateMapEval(goldStandard, candidateMap)

    println("* Input Info *")
    println("(# of GS annotations) = " + eval.getNumOfGoldStandardsAnnotations)
    println("(# of not duplicated sf at GS) = " + eval.getNumOfGoldStandardsSfWithoutDuplication)
    println("(# of not duplicated sf at CM) = " + eval.getNumOfCandidateMapSurfaceForms)

    println("* Metrics *")
    println("(# of GS annotations that the sf is at the CM) = " + eval.getNumOfGoldStandardsSfAtCandidateMap)
    println("(# of GS annotations that the sf is at the CM \nand the annotation resource is one candidate for this sf) = " + eval.getNumOfGoldStandardsURIsAtRespectiveCandidateURIs)
    println("(# of GS annotations that the sf is at the CM, \nbut the annotation resource is NOT one candidate for this sf) = " + eval.getNumOfGoldStandardsURIsNotAtRespectiveCandidateURIs)
    println("(# of GS annotations that the sf is NOT at the CM) = " + eval.getNumOfGoldStandardsSfNotAtCandidateMap)

    println("** Recall **")
    println("Surface Forms Recall (Ration of sf from Corpus at CM by not duplicated sf of Corpus) = " + eval.surfaceFormsRecall)
    println("Resources Recall (Ration of retrieved resources by retrieved sf) = " + eval.resourcesRecall)
  }

}

class GoldStandardEssentials(private val annotatedCorpus: List[AnnotatedParagraph]){  
  /* annotations are pair of (one sf, one resource). The same sf can occur in several annotations */
  val annotations: List[GoldStandardAnnotationEssentials] = {
    var acc: List[GoldStandardAnnotationEssentials] = List()
    annotatedCorpus.foreach(_.occurrences.foreach{ occ =>
      acc = acc :+ new GoldStandardAnnotationEssentials(occ.surfaceForm, occ.resource)
    })
    acc
  }
}
object GoldStandardEssentials{
  def buildFromMilneWittenCorpus(corpusDir: File): GoldStandardEssentials = {
    var annotatedCorpus: List[AnnotatedParagraph] = List()
    MilneWittenCorpus.fromDirectory(corpusDir).foreach{ occ =>
      annotatedCorpus = annotatedCorpus :+ occ
    }
    new GoldStandardEssentials(annotatedCorpus)
  }
}
class GoldStandardAnnotationEssentials(val surfaceForm: SurfaceForm, val resource: DBpediaResource){}

class CandidateMapEssentials(private val candidateMapFile: File){
    /* candidates are pair of (one sf, multiples resources). There is just one candidate per sf. */
    val candidates: List[CandidateEssentials] = {
      var acc: List[CandidateEssentials] = List()
      /* Load the essential candidate map columns (sf and uri) from the candidate map file. Unifying all lines with
         the same  surface form, linking it to its all uris (candidates). */
      Source.fromFile(candidateMapFile).getLines().foreach{ line =>
        val candidateArray: Array[String] = line.split("\t")
        if(candidateArray.length >= 2){
          val lineSf: SurfaceForm = new SurfaceForm(candidateArray(1))
          breakable {
              acc.foreach{ candidate =>
              if(candidate.surfaceForm.equals(lineSf)){
                candidate.addResource(new DBpediaResource(candidateArray(0)))
                break()
              }
            }
            //If do not break  then the lineSf is different of every candidateSf in acc. So, a candidate for it must be created and the first resource must be added.
            acc = acc :+ new CandidateEssentials(lineSf)
            acc(acc.length-1).addResource(new DBpediaResource(candidateArray(0)))
          }
        }else
          SpotlightLog.warn(this.getClass, "Disregarding invalid candidate: %s", line)
      }
      acc
    }
}
class CandidateEssentials(val surfaceForm: SurfaceForm){
  private var resources: List[DBpediaResource] = List()

  def addResource(resource: DBpediaResource) {
    resources = resources :+ resource
  }

  def getResources: List[DBpediaResource] = resources
}