/* Copyright 2012 Intrinsic Ltda.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* Check our project website for information on how to acknowledge the
* authors and how to contribute to the project:
* http://spotlight.dbpedia.org
*
*/

package org.dbpedia.spotlight.util

import org.apache.commons.logging.LogFactory
import com.hp.hpl.jena.rdf.model.{StmtIterator, Model}
import ArrayUtils._
import org.apache.commons.io.FileUtils
import org.dbpedia.spotlight.util.TDBHelper._
import com.hp.hpl.jena.util.FileManager

class ExtractLabels {

  // A method to extract the labels in every language related to resources of the main language. We use three files,
  // an interlanguage links file, a page links file and a labels file.
  def ExtractAllLangsLabels(aMainLanguage: String) {
    println("Starting labels extraction using all languages...")
    // Get the information we need from the companion object
    val mainLanguage = ComplementTypes.mainLanguage(0)
    val allLangsArray = ComplementTypes.allLangsArray
    val labelsNamesArray = ComplementTypes.instTypesNamesArray
    val fileIteratorArray = ComplementTypes.fileIteratorArray
    val tdbStoreArray = ComplementTypes.tdbStoreArray
    val dbpediaBaseDir = ComplementTypes.dbpediaBaseDir
    val tdbStoreBaseDir = ComplementTypes.tdbStoreBaseDir
    val outputBaseDir = ComplementTypes.outputBaseDir

    // Every time this method is called we delete the TDB store files for the main language and create new ones. This happens
    // because as we generate statements and add them to the main language instance types file, we also modify the model
    // for this language. If we do not delete the files Jena would create a model using the old files. This also guarantees
    // that no NULL pointer exceptions will be thrown at this point
    FileUtils.cleanDirectory(new java.io.File(tdbStoreBaseDir + mainLanguage + "/TDB"))

    var i = 0
    for( langName <- allLangsArray ){
      labelsNamesArray(i) = "labels_" + langName + ".nt"
      println(labelsNamesArray(i))
      println(tdbStoreBaseDir + langName + "/TDB")
      println(dbpediaBaseDir + langName + '/' + labelsNamesArray(i))
      tdbStoreArray(i) = createModel(tdbStoreBaseDir + langName + "/TDB")
      FileManager.get().readModel( tdbStoreArray(i), dbpediaBaseDir + langName + '/' + labelsNamesArray(i), "N-TRIPLES" )
      if (i > 0) {
        println(outputBaseDir + mainLanguage + '/' + allLangsArray(0) + '_' + langName + '_' + "links.nt")
        fileIteratorArray(i-1) = createNTFileIterator(outputBaseDir + mainLanguage + '/' + allLangsArray(0) + '_' + langName + '_' + "links.nt")
      }
      i += 1
    }
  }
}

object ExtractLabels {
  private val LOG = LogFactory.getLog(this.getClass)

  val aLabelManager = new ExtractLabels()
  val config = new IndexingConfiguration()

  // Read in from the indexing.properties files all the data we need. Creates arrays accordingly
  val mainLanguage = Array(config.get("org.dbpedia.spotlight.language_i18n_code", ""))
  val otherLangsArray = config.get("org.dbpedia.spotlight.all_languages", "").split(",").toArray
  val allLangsArray = mainLanguage ++ otherLangsArray

  // Checks if the number of languages is not valid
  testArrayLength(1, allLangsArray.length, LOG)

  // Get the base directories used in this process. The user can set the paths to them in the indexing.properties file.
  // If the download.sh script was executed with the complement types option, all the needed directories were already created
  // and are defined inside the script.
  val tdbStoreBaseDir = config.get("org.dbpedia.spotlight.data.tdbStoreBaseDir","")
  val dbpediaBaseDir = config.get("org.dbpedia.spotlight.data.dbpediaBaseDir","")
  val outputBaseDir = config.get("org.dbpedia.spotlight.data.outputBaseDir","")
  val freebaseBaseDir = config.get("org.dbpedia.spotlight.data.freebaseBaseDir","")

  // Creates an array to hold all the names of the instance types files we are going to need
  val labelsNamesArray = new Array[String](allLangsArray.length)
  // Creates an array to hold all models so we can query the instance types files from all the required languages
  val tdbStoreArray = new Array[Model](allLangsArray.length)
  // Creates an iterator array so we can check every resource of the main language related to each complement language
  val fileIteratorArray = new Array[StmtIterator](allLangsArray.length-1)

  // Checks if the number of files to iterate through is not valid
  testArrayLength(1, fileIteratorArray.length, LOG)

  def main(args : Array[String]) {
    aLabelManager.ExtractAllLangsLabels(mainLanguage(0))
  }
}
