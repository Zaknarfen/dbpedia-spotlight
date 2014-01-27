package org.dbpedia.spotlight.util

import java.io.File
import org.dbpedia.spotlight.corpus.MilneWittenCorpus
import org.dbpedia.spotlight.model.{DBpediaResource, SurfaceForm, AnnotatedParagraph}
import scala.io.Source
import org.dbpedia.spotlight.log.SpotlightLog

/**
 * Evaluations class for the candidate maps extracted (by ExtractCandidateMap class) from different sources,
 * to define which sources combination is the best to use in Spotlight.
 *
 * @author Alexandre Cançado Cardoso - accardoso
 */

class CandidateMapEval(goldStandard: GoldStandard, candidateMap: CandidateMap){
  var numOfExistingCandidates = 0
  var numOfNotExistingCandidates = 0

  //todo evaluation metrics
}


object CandidateMapEval {

  def main(args: Array[String]){
    val candidateMapPath: String = args(0)
    val candidateMap: CandidateMap = new CandidateMap(candidateMapPath)

    val annotatedCorpusPath: String = "/home/alexandre/intrinsic/corpus/MilneWitten-wikifiedStories"//args(1)
    var annotatedCorpus: List[AnnotatedParagraph] = List()
    MilneWittenCorpus.fromDirectory(new File(annotatedCorpusPath)).foreach{ occ =>
      annotatedCorpus = annotatedCorpus :+ occ
    }
    val goldStandard: GoldStandard = new GoldStandard(annotatedCorpus)

    val eval = new CandidateMapEval(goldStandard, candidateMap)

    println(eval)
  }

}

class GoldStandard(annotatedCorpus: List[AnnotatedParagraph]){
  //The annotationsUri(i) and the annotationsSf(i) are the respective uri and surface form of the ith annotation 
  private var annotationsURI: List[DBpediaResource] = List()
  private var annotationsSf: List[SurfaceForm] = List()

  annotatedCorpus.foreach(_.occurrences.foreach{ occ =>
    annotationsURI = annotationsURI :+ occ.resource
    annotationsSf = annotationsSf :+ occ.surfaceForm
  })

  def getAnnotationsURI() = annotationsURI
  def getAnnotationsSf() = annotationsSf
}

class CandidateMap(candidateMapFilePath: String){
  private var candidatesURI: List[DBpediaResource] = List()
  private var candidatesSf: List[SurfaceForm] = List()
  private var candidatesCount: List[Int] = List()
  private var candidatesSource: List[String] = List()

  private var numOfCandidates: Int = 0

  Source.fromFile(candidateMapFilePath).getLines().foreach{ line =>
    val candidateArray: Array[String] = line.split("\t")
    if(candidateArray.length == 4){
      candidatesURI = candidatesURI :+ new DBpediaResource(candidateArray(0))
      candidatesSf = candidatesSf :+ new SurfaceForm(candidateArray(1))
      candidatesCount = candidatesCount :+ candidateArray(2).toInt
      candidatesSource = candidatesSource :+ candidateArray(3)

      numOfCandidates += 1
    }else
      SpotlightLog.warn(this.getClass, "Disregarding invalid candidate: %s", line)
  }

  def getCandidatesURI() = candidatesURI
  def getCandidatesSf() = candidatesSf
  def getCandidatesCount() = candidatesCount
  def getCandidatesSource() = candidatesSource

  def getNumOfCandidates() = numOfCandidates

  override def toString(): String = {
    var cm: List[String] = List()

    for(i<- 0 to numOfCandidates){
      cm = cm :+ List(candidatesURI(i).uri, candidatesSf(i).name, candidatesCount(i), candidatesSource(i)).mkString("\t")
    }

    cm.mkString("\n")
  }
}