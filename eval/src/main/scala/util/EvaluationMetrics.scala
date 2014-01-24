package org.dbpedia.spotlight.util

/**
 * Implements some useful metrics to evaluations.
 * This was defined as in the article: Ben Hachey, Will Radford, Joel Nothman, Matthew Honnibal and James R. Curran (2012).
 * "Evaluating Entity Linking with Wikipedia." Artificial Intelligence, 194: 130-150.
 * Available at: http://benhachey.info/pubs/hachey-aij12-evaluating.pdf
 *
 * @author Alexandre Cançado Cardoso - accardoso
 */

object EvaluationMetrics {

  def accuracy(goldStandardAnotations: List, candidateList: List) = {
    val n = goldStandardAnotations.length
    if(candidateList.length != n)
      throw new IllegalArgumentException("Both sets must be of the same size.")
    
    var A = 0
    candidateList.foreach{ candidate =>
      goldStandardAnotations.contains(candidate)
      A += 1
    } 
    A = A / n    
  }
  
  def candidateCount(candidateList: List[List]) = {
    var C = 0
    candidateList.foreach{ candidate =>
      C += candidate.length      
    }
    C = C / candidateList.length
  }
  
  def candidatePrecision(goldStandardList: List, candidateList: List) = {
    
  }

}
