/*
 * Copyright 2012 DBpedia Spotlight Development Team
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Check our project website for information on how to acknowledge the authors and how to contribute to the project: http://spotlight.dbpedia.org
 */

package org.dbpedia.spotlight.util

import scala.util.matching.Regex
import org.dbpedia.spotlight.model.{Factory, SpotlightConfiguration}
import scala.io.Source
import java.io.{FileWriter, FileInputStream, PrintStream, File}
import org.dbpedia.spotlight.lucene.index.ExtractOccsFromWikipedia
import org.dbpedia.spotlight.log.SpotlightLog
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.semanticweb.yars.nx.parser.NxParser
import java.text.Normalizer
import org.apache.commons.lang.StringUtils
import java.text.Normalizer.Form
import com.google.code.externalsorting.ExternalSort

/**
 * Functions to create Concept URIs (possible mainResources of disambiguations)
 *                     transitive closure of redirects that end at Concept URIs
 *                     surface forms for Concept URIs
 * from DBpedia data sets and Wikipedia.
 *
 * Contains logic of what to index wrt. URIs and SurfaceForms.
 *
 * @author maxjakob
 * @author pablomendes (created blacklisted URI patterns for language-specific stuff (e.g. List_of, etc.)
 * @author Renan Dembogurski - zaknarfen
 * @author Alexandre Cançado Cardoso - accardoso
 */

object ExtractCandidateMap {

  //List of the code of every possible candidate mapping sources to be used
  val candidateMappingSources: List[Char] = List[Char]('T','R','D','O','M') //T->titles; R->redirects; D->disambiguations; O->occurrences; M->mapping-based properties

  //ExtractCandidatesMap Input Files
  private var titlesFileName: String = ""
  private var redirectsFileName: String = ""
  private var disambiguationsFileName: String = ""

  //ExtractCandidatesMap Output Files
  private var conceptURIsFileName: String = ""
  private var redirectTCFileName: String= ""
  private var occsFileName: String = ""
  private var surfaceFormsFileName: String = ""

  //ExtractCandidatesMap options/configurations
  private var config : IndexingConfiguration = null
  private var language: String = ""
  private var blacklistedURIPatterns: Set[Regex] = Set[Regex]()
  private var stopWords: Set[String] = Set[String]()
  private var maximumSurfaceFormLength: Int = 50


  /* Validate the list of candidate mapping sources to be used */
  private def validateCandidateMappingSourcesToUse(candidateMappingSourcesToUse: List[Char]) {
    if(!(candidateMappingSourcesToUse.length <= candidateMappingSources.length && candidateMappingSourcesToUse.length > 0))
      throw new IllegalArgumentException("Invalid number of candidate mapping sources was informed. The codes of the registered sources are: %s".format(candidateMappingSources.mkString(", ")))
    if(!candidateMappingSourcesToUse.forall(candidateMappingSources.contains(_)))
      throw new UnsupportedOperationException("At least one informed code at the candidate mapping source list is not from a valid source. The codes of the registered sources are: %s".format(candidateMappingSources.mkString(", ")))
  }

  /* Obtain the concept URIs from the input file and save then at conceptURIsFileName */
  private def extractConceptURIs() {
    if (!new File(titlesFileName).isFile || !new File(redirectsFileName).isFile || !new File(disambiguationsFileName).isFile) {
      throw new IllegalStateException("labels, redirects or disambiguations file not set")
    }

    val badURIsFile = conceptURIsFileName+".NOT"
    SpotlightLog.info(this.getClass, "Creating concept URIs file %s ...", conceptURIsFileName)

    val conceptURIStream = new PrintStream(conceptURIsFileName, "UTF-8")
    val badURIStream = new PrintStream(badURIsFile, "UTF-8")
    var badURIs = Set[String]()

    SpotlightLog.info(this.getClass, "  collecting bad URIs from redirects in %s and disambiguations in %s ...", redirectsFileName, disambiguationsFileName)
    // redirects and disambiguations are bad URIs
    val pattern = new Regex("""(\w*)/resource/(\w*)""", "stringToReplace", "uri")
    for (fileName <- List(redirectsFileName, disambiguationsFileName)) {
      val input = new BZip2CompressorInputStream(new FileInputStream(fileName),true)
      //for(triple <- new NxParser(input)
      val parser = new NxParser(input)
      while (parser.hasNext) {
        val triple = parser.next
        try {
          val badUri = pattern.findFirstMatchIn(triple(0).toString).get.group("uri")
          badURIs += badUri
          badURIStream.println(badUri)
        } catch {
          case e: Exception => {
            SpotlightLog.info(this.getClass, "String in the wrong format skipped! ") // In case a string not containing a /resource/ is passed
          }
        }
      }
      input.close()
    }
    badURIStream.close()

    SpotlightLog.info(this.getClass, "  collecting concept URIs from titles in %s, without redirects and disambiguations...", titlesFileName)
    val titlesInputStream = new BZip2CompressorInputStream(new FileInputStream(titlesFileName), true)
    // get titles without bad URIs
    val parser = new NxParser(titlesInputStream)
    while (parser.hasNext) {
      val triple = parser.next
      try {
        val uri = pattern.findFirstMatchIn(triple(0).toString).get.group("uri")
        if (looksLikeAGoodURI(uri) && !badURIs.contains(uri)) {
          conceptURIStream.println(uri)
        }
      } catch {
        case e: Exception => {
          SpotlightLog.info(this.getClass, "String in the wrong format skipped! ") // In case a string not containing a /resource/ is passed
        }
      }
    }
    conceptURIStream.close()
    titlesInputStream.close()

    SpotlightLog.info(this.getClass, "Done.")
    //        conceptURIsFileName = conceptURIsFile.getAbsolutePath
    //        IndexConfiguration.set("conceptURIs", conceptURIsFileName)
  }
  /* Verify if the informed uri is (looks like) a good uri */
  private def looksLikeAGoodURI(uri : String) : Boolean = {
    // cannot contain a slash (/)
    if (uri contains "/")
      return false
    // cannot contain a hash (#)
    if (uri contains "%23") //TODO re-evaluate this decision in context of DBpedia 3.7
      return false
    // has to contain a letter
    if ("""^[\\W\\d]+$""".r.findFirstIn(uri) != None)
      return false
    // cannot be a list, or any other pattern specified in the blacklist
    blacklistedURIPatterns.foreach(p => if (p.pattern.matcher(uri).matches) return false) // generalizes: if (uri contains "List_of_") return false
    true
  }

  /* Obtain the redirects transitive closure from the input files and save it at redirectTCFileName */
  private def extractRedirectsTransitiveClosure() {
    if (!new File(redirectsFileName).isFile) {
      throw new IllegalStateException("redirects file not set")
    }
    if (!new File(conceptURIsFileName).isFile) {
      throw new IllegalStateException("concept URIs not created yet; call saveConceptURIs first or set concept URIs file")
    }
    SpotlightLog.info(this.getClass, "Creating redirects transitive closure file %s ...", redirectsFileName)

    SpotlightLog.info(this.getClass, "  loading concept URIs from %s...", conceptURIsFileName)
    val conceptURIs = Source.fromFile(conceptURIsFileName, "UTF-8").getLines().toSet

    SpotlightLog.info(this.getClass, "  loading redirects from %s...", redirectsFileName)
    var linkMap = Map[String,String]()
    val redirectsInput = new BZip2CompressorInputStream(new FileInputStream(redirectsFileName), true)

    val parser = new NxParser(redirectsInput)
    val pattern = new Regex("""(\w*)/resource/(\w*)""", "stringToReplace", "uri")
    while (parser.hasNext) {
      val triple = parser.next
      try {
        val subj = pattern.findFirstMatchIn(triple(0).toString).get.group("uri")
        val obj = pattern.findFirstMatchIn(triple(2).toString).get.group("uri")
        linkMap = linkMap.updated(subj, obj)
      } catch {
        case e: Exception => {
          SpotlightLog.info(this.getClass,"String in the wrong format skipped! ") // In case a string not containing a /resource/ is passed
        }
      }
    }
    redirectsInput.close()

    val redURIstream = new PrintStream(redirectTCFileName, "UTF-8")

    SpotlightLog.info(this.getClass, "  collecting redirects transitive closure...")
    for (redirectUri <- linkMap.keys) {
      val endUri = getEndOfChainUri(linkMap, redirectUri, 0)
      if (conceptURIs contains endUri) {
        redURIstream.println(redirectUri+"\t"+endUri)
      }
    }

    redURIstream.close()
    SpotlightLog.info(this.getClass, "Done.")
    //        redirectTCFileName = redirectTCFileName.getAbsolutePath
    //        IndexConfiguration.set("preferredURIs", redirectTCFileName)
  }
  /* Traverse the URIs chain to define its end */
  private def getEndOfChainUri(m : Map[String,String], k : String, acc : Int) : String = {
    // get end of chain but check for redirects to itself
    m.get(k) match {
      case Some(s : String) => {
        if (normalizeString(s).toLowerCase equals normalizeString(k).toLowerCase) k
        else {
          if (normalizeString(s).toLowerCase.contains(normalizeString(k).toLowerCase)) {
            s
          } else if (normalizeString(k).toLowerCase.contains(normalizeString(s).toLowerCase)) {
            k
            //TODO: test this if k or s, stop condition
          } else if (acc >= 20) {
            s
          } else {
            getEndOfChainUri(m, s, acc + 1)
          }
        }
      }
      case None => k
    }
  }
  /* Remove special chars */
  private def normalizeString(aString: String): String = {
    var searchList: Array[String] = Array()
    var replaceList: Array[String] = Array()
    language match {
      case "portuguese" => {
        searchList = Array("Á" ,"À" ,"Ã" ,"É" ,"Ê" ,"Í" ,"Ó" ,"Ô" ,"Õ" ,"Ú" ,"Ü" ,"Ç")
        replaceList = Array("A" ,"A" ,"A" ,"E" ,"E" ,"I" ,"O" ,"O" ,"O" ,"U" ,"U" ,"C")
        searchList = searchList ++ searchList.map(_.toLowerCase).toList
        replaceList = replaceList ++ replaceList.map(_.toLowerCase).toList
      }
      case "german" | _ => {
        searchList = Array("Ä", "ä", "Ö", "ö", "Ü", "ü", "ß")
        replaceList = Array("Ae", "ae", "Oe", "oe", "Ue", "ue", "ss")
      }
    }

    if (aString == null) ""
    else Normalizer.normalize(StringUtils.replaceEachRepeatedly(aString, searchList, replaceList), Form.NFD).replaceAll("[^\\p{ASCII}]", "")
  }

  /* Extract the candidates from the titles and save them */
  private def extractCandidatesFromTitles(titlesCandidatesFileName: String, lowerCased : Boolean=false): File = {
    if (!new File(conceptURIsFileName).isFile) {
      throw new IllegalStateException("concept URIs not created yet; call saveConceptURIs first or set concept URIs file")
    }

    val candidatesT: File = new File(titlesCandidatesFileName)

    val stream = new PrintStream(candidatesT, "UTF-8")
    //All titles of concept URIs are surface forms
    for (conceptUri <- Source.fromFile(conceptURIsFileName, "UTF-8").getLines()) {
      getCleanSurfaceForm(conceptUri, stopWords, lowerCased) match {
        case Some(sf : String) => stream.println(conceptUri+"\t"+sf)
        case None => SpotlightLog.debug(this.getClass, "Concept URI '%s' does not decode to a good surface form", conceptUri)
      }
    }

    candidatesT
  }
  /* Returns a cleaned surface form if it is considered to be worth keeping */
  private def getCleanSurfaceForm(surfaceForm : String, stopWords : Set[String], lowerCased : Boolean=false) : Option[String] = {
    val cleanedSurfaceForm = Factory.SurfaceForm.fromWikiPageTitle(surfaceForm, lowerCased).name
    if (isGoodSurfaceForm(cleanedSurfaceForm, stopWords)) Some(cleanedSurfaceForm) else None
  }
  /* Verify if the surface form is a good one */
  private def isGoodSurfaceForm(surfaceForm : String, stopWords : Set[String]) : Boolean = {
    // not longer than limit
    if (surfaceForm.length > maximumSurfaceFormLength) {
      return false
    }
    // contains a letter
    if ("""^[\W\d]+$""".r.findFirstIn(surfaceForm) != None) {
      return false
    }
    // not an escaped char. see http://sourceforge.net/mailarchive/message.php?msg_id=28908255
    if ("""\\\w""".r.findFirstIn(surfaceForm) != None) {
      return false
    }
    // contains a non-stopWord  //TODO Remove when case sensitivity and common-word/idiom detection is in place. This restriction will eliminate many works (books, movies, etc.) like Just_One_Fix, We_Right_Here, etc.
    if (stopWords.nonEmpty
      && surfaceForm.split(" ").filterNot(
      sfWord => stopWords.map(stopWord => stopWord.toLowerCase)
        contains
        sfWord.toLowerCase
    ).isEmpty) {
      return false
    }
    true
  }

  /* Extract the candidates from the redirects and save them */
  private def extractCandidatesFromRedirects(redirectsCandidatesFileName: String, lowerCased : Boolean=false): File = {
    if (!new File(redirectsFileName).isFile) {
      throw new IllegalStateException("redirects file not set")
    }

    extractCandidatesFromRedirectsOrDisambiguations(redirectsCandidatesFileName, redirectsFileName, lowerCased)
  }

  /* Extract the candidates from the disambiguations and save them */
  private def extractCandidatesFromDisambiguations(disambsCandidatesFileName: String, lowerCased : Boolean=false): File = {
    if (!new File(disambiguationsFileName).isFile) {
      throw new IllegalStateException("disambiguations file not set")
    }

    extractCandidatesFromRedirectsOrDisambiguations(disambsCandidatesFileName, disambiguationsFileName, lowerCased)
  }

  /* Extractor for both redirects/disambiguations */
  private def extractCandidatesFromRedirectsOrDisambiguations(specificCandidatesFileName: String, sourceFileName: String, lowerCased : Boolean=false): File = {
    if (!new File(conceptURIsFileName).isFile) {
      throw new IllegalStateException("concept URIs not created yet; call saveConceptURIs first or set concept URIs file")
    }

    var conceptURIs = Set[String]()
    for (conceptUri <- Source.fromFile(conceptURIsFileName, "UTF-8").getLines()) {
      conceptURIs += conceptUri
    }
    val pattern = new Regex("""(\w*)/resource/(\w*)""", "stringToReplace", "uri")
    val stream = new PrintStream(specificCandidatesFileName, "UTF-8")
    for (fileName <- List((new File(sourceFileName)))) {
      val input = new BZip2CompressorInputStream(new FileInputStream(fileName), true)
      val parser = new NxParser(input)
      while (parser.hasNext) {
        val triple = parser.next
        try {
          val surfaceFormUri = pattern.findFirstMatchIn(triple(0).toString).get.group("uri")
          val uri = pattern.findFirstMatchIn(triple(2).toString).get.group("uri")

          if (conceptURIs contains uri) {
            getCleanSurfaceForm(surfaceFormUri, stopWords, lowerCased) match {
              case Some(sf : String) => stream.println(uri+"\t"+sf)
              case None =>
            }
          }
        } catch {
          case e: Exception => {
            SpotlightLog.info(this.getClass, "String in the wrong format skipped! ") // In case a string not containing a /resource/ is passed
          }
        }
      }
      input.close()
    }
    stream.close()

    new File(specificCandidatesFileName)
  }

  /* Extract the candidates from the occurrences and save them */
  private def extractCandidatesFromOccs(occsCandidatesFileName: String, lowerCased : Boolean=false): File = {
    val candidateO: File = new File(occsCandidatesFileName)

    val stream = new PrintStream(candidateO, "UTF-8")
    for (line <- Source.fromFile(occsFileName).getLines()) {
      val lineArray = line.split("\t")
      stream.println(lineArray(2) + "\t" + lineArray(1)) //line content structure: id \t surface form \t entity uri \t ...
    }
    stream.close()

    candidateO
  }

  /* Extract the candidates from the mapping based properties and save them */
  private def extractCandidatesFromMappingBasedProperties(mapBasedPropsCandidatesFileName: String, lowerCased : Boolean=false): File = {
    val candidateM: File = new File(mapBasedPropsCandidatesFileName)

    //TODO - extract the candidates from the "naming properties" from mapping_based properties (this code has not been shared yet)
    //As it can not be implemented yet this method return an empty file, simulating that the extraction generate no candidate
    val writer = new FileWriter(candidateM, false)
    writer.write("")
    writer.close()

    candidateM
  }

  /* Define the count column with the number of repetitions of the same candidate are and remove the duplicates */
  private def constructCountColumn(candidateMap: File){
    val candidateMapFileNameArray: Array[String] = candidateMap.getCanonicalPath.split('.')
    val withoutCountCandidateMapFile: File = new File(candidateMapFileNameArray.slice(0, candidateMapFileNameArray.length-1).mkString(".")+".withoutCount."+candidateMapFileNameArray(candidateMapFileNameArray.length-1)+".tmp")

    //Replace the informed candidateMap by itself sorted
    sortCandidateMap(candidateMap)

    //Back up the original (already sorted) candidateMap file
    if(!candidateMap.renameTo(withoutCountCandidateMapFile))
      throw new IllegalAccessException("Could not rename the file %s to %s".format(candidateMap.getCanonicalPath, withoutCountCandidateMapFile.getCanonicalPath))

    //Count the candidates repetition put it at the new count column and remove candidate duplicates
    try{
      val mapCandidatesIterator = Source.fromFile(withoutCountCandidateMapFile).getLines()
      //Initialize candidates fields with the first line candidate and the counting variable to count this one
      var currentCandidateFields: Array[String] = mapCandidatesIterator.take(1).mkString("").split("\t")
      var count:Int = 1
      //Set the output to the candidateMap (which is empty)
      val stream = new PrintStream(candidateMap, "UTF-8")
      //Transverse candidate map counting every candidate (it skip the first line/candidate as it was already read at currentCandidateFields and count initialization)
      mapCandidatesIterator.foreach{ line =>
        val lineCandidateFields: Array[String] = line.split("\t")
        //Get the line candidate uri and surface form and treat if it does not have one or both
        var lineUri: String = ""
        var lineSf: String = ""
        try{
          lineUri = lineCandidateFields(0)
          lineSf = lineCandidateFields(1)
        }catch{
          case e: ArrayIndexOutOfBoundsException =>{
            if(currentCandidateFields.length < 2)
              throw e
            e.getMessage match {
              case "0" | null => //It is a empty line, so move on
              case "1" => {
                SpotlightLog.warn(this.getClass, "Invalid entry: No surface form candidate ( uri = %s ) in candidate map: %s",
                  lineCandidateFields(0), withoutCountCandidateMapFile.getCanonicalPath)
                SpotlightLog.warn(this.getClass, "The invalid entry was discarded.")
              }
              case _ => throw e
            }
          }
        }
        //If the line candidate is valid (the lineUri and the lineSf were set)
        if(lineSf != "" && lineUri!= ""){
          //If the candidates both surface forms and URIs are equal then they are the same candidate
          if(currentCandidateFields(0).equals(lineUri) && currentCandidateFields(1).equals(lineSf)){
            //Count 1 more of the same candidate and do not print the duplicate
            count +=1
          }else { //As the input map is sorted, when the next candidate differs from the current one, there are no more equal candidate in the map.
            if(!currentCandidateFields.isEmpty){
              //Print the candidate (just one time) and its count of how many time had occur
              stream.println((currentCandidateFields :+ count).mkString("\t"))
            }
            //The new current candidate is the line candidate
            currentCandidateFields = lineCandidateFields
            //Start the counting for the new current candidate
            count = 1
          }
        }
      }
      stream.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception("Could not construct the count column (count the candidates repetitions and remove its duplicates) at the file %s . The original file is at %s".format(candidateMap.getCanonicalPath, withoutCountCandidateMapFile.getCanonicalPath))
      }
    }

    //As everything run ok, delete the original candidate map back up
    if(!(new File(withoutCountCandidateMapFile.getCanonicalPath)).delete() && (new File(withoutCountCandidateMapFile.getCanonicalPath)).exists())
      SpotlightLog.warn(this.getClass(), "Could not delete the temporary file with candidate map without the count column (and with duplicates) at: %s", withoutCountCandidateMapFile.getCanonicalPath)
  }
  /* Replace the candidateMap file by itself sorted */
  private def sortCandidateMap(candidateMap: File){
    val candidateMapFileNameArray: Array[String] = candidateMap.getCanonicalPath.split('.')
    val unsortedCandidateMap: File = new File(candidateMapFileNameArray.slice(0, candidateMapFileNameArray.length-1).mkString(".")+".unsorted."+candidateMapFileNameArray(candidateMapFileNameArray.length-1)+".tmp")

    //Back up the original candidateMap
    if(!candidateMap.renameTo(unsortedCandidateMap))
      throw new IllegalAccessException("Could not rename the file %s to %s".format(candidateMap.getCanonicalPath, unsortedCandidateMap.getCanonicalPath))

    //Sort the original candidate map at unsortedCandidateMap file into the candidateMap file
    try{
      //Use ExternalSort to sort the candidate map
      ExternalSort.sort(unsortedCandidateMap, candidateMap)
    } catch {
      case e: Exception => throw new Exception("Could not sort the file %s . The original unsorted file is at %s"
        .format(candidateMap.getCanonicalPath, unsortedCandidateMap.getCanonicalPath))
    }

    //As everything run ok, delete the original candidate map back up
    if(!unsortedCandidateMap.delete() && unsortedCandidateMap.exists())
      SpotlightLog.warn(this.getClass(), "Could not delete the temporary file with an unsorted partial candidate map at: %s", unsortedCandidateMap.getCanonicalPath)
  }

  /* Merge all candidate maps, listing at a new last column the sources */
  private def mergeCandidateMaps(candidateMaps: List[File], candidateMapsSourceCodes: List[Char]): File = {
    //Verify if for each candidate map there is a respective code (and if there is no extra code)
    if(candidateMaps.length != candidateMapsSourceCodes.length)
      throw new IllegalArgumentException("The candidateMaps files list and its respective codes list must be of the same length.")

    /* Merge pairs of candidates map files recursively into a new file */
    def recursiveMerge(candidateMaps: List[File], candidateMapsSourceCodes: List[Char]): File = {
      var mergedCandidateMap: File = new File(surfaceFormsFileName+".merge_of_"+candidateMapsSourceCodes.mkString("")+"_maps.tmp")

      //Stop criterion: 1 map only
      if(candidateMaps.length == 1){
        val stream = new PrintStream(mergedCandidateMap, "UTF-8")
        Source.fromFile(candidateMaps(0)).getLines().foreach{ line =>
          stream.println(line+"\t"+candidateMapsSourceCodes(0).toString.toUpperCase)
        }
        stream.close
      }else{
        sortCandidateMap(candidateMaps(0))
        val tailMaps:File = recursiveMerge(candidateMaps.slice(1, candidateMaps.length), candidateMapsSourceCodes.slice(1, candidateMaps.length))
        sortCandidateMap(tailMaps)
        val headMapSourceCode = candidateMapsSourceCodes(0).toString().toUpperCase

        val headCandidates = Source.fromFile(candidateMaps(0)).getLines()
        var headCurrentCandidateFields: Array[String] = Array()
        var headMapHasFinished: Boolean = false
        if(!headCandidates.isEmpty)
          headCurrentCandidateFields = headCandidates.next().split("\t")
        else
          headMapHasFinished = true

        val tailCandidates = Source.fromFile(tailMaps).getLines()
        var tailCurrentCandidateFields: Array[String] = Array()
        var tailMapHasFinished: Boolean = false
        if(!tailCandidates.isEmpty)
          tailCurrentCandidateFields = tailCandidates.next().split("\t")
        else
          tailMapHasFinished = true

        val stream = new PrintStream(mergedCandidateMap, "UTF-8")
        while(!headMapHasFinished && !tailMapHasFinished){
          headCurrentCandidateFields(0).compareTo(tailCurrentCandidateFields(0)) match {
            case x if x < 0 => {
              stream.println(headCurrentCandidateFields.mkString("\t") + "\t" + headMapSourceCode)
              if(!headCandidates.isEmpty)
                headCurrentCandidateFields = headCandidates.next().split("\t")
              else
                headMapHasFinished = true
            }
            case x if x > 0 => {
              stream.println(tailCurrentCandidateFields.mkString("\t"))
              if(!tailCandidates.isEmpty)
                tailCurrentCandidateFields = tailCandidates.next().split("\t")
              else
                tailMapHasFinished = true
            }
            case 0 => {
              headCurrentCandidateFields(1).compareTo(tailCurrentCandidateFields(1)) match {
                case x if x < 0 => {
                  stream.println(headCurrentCandidateFields.mkString("\t") + "\t" + headMapSourceCode)
                  if(!headCandidates.isEmpty)
                    headCurrentCandidateFields = headCandidates.next().split("\t")
                  else
                    headMapHasFinished = true
                }
                case x if x > 0 => {
                  stream.println(tailCurrentCandidateFields.mkString("\t"))
                  if(!tailCandidates.isEmpty)
                    tailCurrentCandidateFields = tailCandidates.next().split("\t")
                  else
                    tailMapHasFinished = true
                }
                case 0 => {
                  //Merge the candidate count
                  val count:Int = headCurrentCandidateFields(2).toInt + tailCurrentCandidateFields(2).toInt
                  //Merge candidate sources
                  val sources:String = headMapSourceCode + tailCurrentCandidateFields(3)
                  //Define the merged candidate entry
                  val outputFields = List[String](tailCurrentCandidateFields(0), tailCurrentCandidateFields(1), count.toString, sources)
                  //Print to the output merged file the merged candidate
                  stream.println(outputFields.mkString("\t"))

                  //Get next candidates from both maps (as they have no duplicates and are ordered)
                  if(!headCandidates.isEmpty)
                    headCurrentCandidateFields = headCandidates.next().split("\t")
                  else
                    headMapHasFinished = true
                  if(!tailCandidates.isEmpty)
                    tailCurrentCandidateFields = tailCandidates.next().split("\t")
                  else
                    tailMapHasFinished = true
                }
              }
            }
          }
        }
        //As the candidates of one of the files has finished then the resting candidates of the other shall be copied to the merged file, without the need of performing the merging verifications
        if(!headMapHasFinished){
          //Copy the resting candidates from the head map into the merged map adding the source column (which going to be only the head map source)
          stream.println(headCurrentCandidateFields.mkString("\t") + "\t" + headMapSourceCode)
          headCandidates.foreach{ line =>
            stream.println(line + "\t" + headMapSourceCode)
          }
        }else if(!tailMapHasFinished){
          stream.println(tailCurrentCandidateFields.mkString("\t"))
          //Copy the resting candidates from the tail maps into the merged map without adding the source column (because as the tail maps were already merged into a temp merged map this candidates already have theirs sources)
          tailCandidates.foreach(stream.println(_))
        }
        stream.close()

        if(!tailMaps.delete)
          SpotlightLog.warn(this.getClass,"Could not delete the temporary candidate map file at: %s", tailMaps.getCanonicalPath)
      }

      mergedCandidateMap
    }

    return recursiveMerge(candidateMaps, candidateMapsSourceCodes)
  }

  def main(args : Array[String]) {
    /* Get the config file */
    val indexingConfigFileName = args(0)
    config = new IndexingConfiguration(indexingConfigFileName)

    /* Get candidates maps selected to be used to make the merged candidates map */
    var candidateMapsCodes: List[Char] = List[Char]()
    // If no source is informed, then use every sources. Otherwise, validate the informed sources
    if(args.length < 2)
      candidateMapsCodes = candidateMappingSources
    else{
      candidateMapsCodes = args(1).toUpperCase.toList
      validateCandidateMappingSourcesToUse(candidateMapsCodes)
    }
    var candidateMapFiles: List[File] = List()

    /* Get input files path (from config file) */
    titlesFileName          = config.get("org.dbpedia.spotlight.data.labels")
    redirectsFileName       = config.get("org.dbpedia.spotlight.data.redirects")
    disambiguationsFileName = config.get("org.dbpedia.spotlight.data.disambiguations")

    /* Get output files path (from config file) */
    conceptURIsFileName     = config.get("org.dbpedia.spotlight.data.conceptURIs")
    redirectTCFileName      = config.get("org.dbpedia.spotlight.data.redirectsTC")
    occsFileName            = config.get("org.dbpedia.spotlight.data.occs")
    surfaceFormsFileName    = config.get("org.dbpedia.spotlight.data.surfaceForms")

    /* Get execution options/configurations (from config file) */
    //Candidates Maps sources language
    language = config.getLanguage().toLowerCase
    //DBpedia resources namespace
    SpotlightConfiguration.DEFAULT_NAMESPACE=config.get("org.dbpedia.spotlight.default_namespace",SpotlightConfiguration.DEFAULT_NAMESPACE)
    //URIs Black List: Bad URIs. (Any URIs that match these patterns shall be excluded. Used for Lists, disambiguations, etc.)
    val blacklistedURIPatternsFileName = config.get("org.dbpedia.spotlight.data.badURIs."+language)
    blacklistedURIPatterns = Source.fromFile(blacklistedURIPatternsFileName).getLines().map( u => u.r ).toSet
    //Bad surface forms: (Any surface form that match these patterns shall be excluded.)
    //  Stopwords (Every stopword is a bad surface form)
    val stopWordsFileName = config.get("org.dbpedia.spotlight.data.stopWords."+language)
    stopWords = Source.fromFile(stopWordsFileName, "UTF-8").getLines().toSet
    //  Maximum surface form length (Any "too long" surface form is a Bad one)
    maximumSurfaceFormLength = config.get("org.dbpedia.spotlight.data.maxSurfaceFormLength").toInt

    /* Pre-process input files */
    //Get concept URIs (and save the respective output file with it)
    extractConceptURIs()
    //Get concept URIs (and save an output file with it)
    extractRedirectsTransitiveClosure()

    /* Extract simple (TRD) candidates */
    //Get candidates from titles
    if(candidateMapsCodes.contains('T'))
      candidateMapFiles = candidateMapFiles :+ extractCandidatesFromTitles(surfaceFormsFileName+".FromTitles.tmp")
    //Get candidates from redirects
    if(candidateMapsCodes.contains('R'))
      candidateMapFiles = candidateMapFiles :+ extractCandidatesFromRedirects(surfaceFormsFileName+".FromRedirects.tmp")
    //Get candidates from disambiguations
    if(candidateMapsCodes.contains('D'))
      candidateMapFiles = candidateMapFiles :+ extractCandidatesFromDisambiguations(surfaceFormsFileName+".FromDisambiguations.tmp")

    /* Extract extra candidates using (Wikipedia) Occurrences */
    if(candidateMapsCodes.contains('O')){
      //Get the Wikipedia occs
      ExtractOccsFromWikipedia.main(Array(indexingConfigFileName, occsFileName))
      //Get candidates from occs
      candidateMapFiles = candidateMapFiles :+ extractCandidatesFromOccs(surfaceFormsFileName+".FromOccs.tmp")
    }

    /* Extract more extra candidates using Mapping-based properties */
    if(candidateMapsCodes.contains('M')){
      //TODO - get the "naming properties" from mapping_based properties (this code has not been shared yet)
      //Get candidates from Mapping-based properties
      candidateMapFiles = candidateMapFiles :+ extractCandidatesFromMappingBasedProperties(surfaceFormsFileName+".FromMappingBasedProps.tmp")
    }

    /* Candidates maps creating the count column */
    //Remove duplicates unifying then and defining each candidate count field
    candidateMapFiles.foreach(constructCountColumn(_))

    /* Merge all candidates maps (and the source column creation) */
    val mergedCandidateMap: File = mergeCandidateMaps(candidateMapFiles, candidateMapsCodes)

    /* Replace the file surfaceFormsFile by the final extracted candidate map */
    val finalCandidateMap: File = new File(surfaceFormsFileName)
    if( (finalCandidateMap.exists() && !finalCandidateMap.delete()) || !mergedCandidateMap.renameTo(finalCandidateMap))
      SpotlightLog.error(this.getClass, "Could not replace the %s by the extracted candidate map, but it is at: %s", finalCandidateMap.getCanonicalPath, mergedCandidateMap)
    else
      SpotlightLog.info(this.getClass, "The candidate map was successfully extracted from the sources: %s. And stored at: %s", candidateMapsCodes.mkString(", "), finalCandidateMap.getCanonicalPath)

    /* Delete temporary candidate maps files */
    candidateMapFiles.foreach{ tempCandidateMap =>
      if(!tempCandidateMap.delete() && tempCandidateMap.exists())
        SpotlightLog.warn(this.getClass, "Could not delete the temporary candidate map file at: %s", tempCandidateMap.getCanonicalPath)
    }

  }

}