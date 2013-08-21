package org.dbpedia.spotlight.lucene.index

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

import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.tdb.TDBFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.ResultSet
import scala.util.matching.Regex
import scala.io.Source
import scala.util.control.Breaks._
import com.hp.hpl.jena.rdf.model._
import java.io._
import java.util.Properties
import org.dbpedia.spotlight.exceptions.ConfigurationException
import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log
import org.apache.commons.io.FileUtils
import com.hp.hpl.jena.vocabulary.RDF
import com.google.api.client.http.GenericUrl
import com.google.api.client.http.javanet.NetHttpTransport
import com.jayway.jsonpath.JsonPath
import org.json.simple.parser.JSONParser
import org.json.simple.JSONObject


class ComplementTypes {
  def testArrayLength(anArrayLength: Int, aLog: Log) {
    if (anArrayLength < 1) {
      aLog.error("At least one language must be supplied to execute this process.")
      System.exit(1)
    }
  }

  def createModel(aDirectory: String): Model = {
    val dataset = TDBFactory.createDataset(aDirectory)
    dataset.getDefaultModel
  }

  def createNTFileIterator(aFile: String): StmtIterator = {
    val input = FileManager.get().open(aFile)
    val model = ModelFactory.createDefaultModel()
    model.read(input, null, "N-TRIPLES")
    model.listStatements()
  }

  // Builds the default query to be used in the select command
  def buildQueryRDFType(aString: String): String = {
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + '\n' +
    "SELECT ?o " + '\n' +
    "WHERE {<" + aString + "> rdf:type ?o}"
  }

  def buildQueryOWLSameAs(aString: String): String = {
    "PREFIX owl: <http://www.w3.org/2002/07/owl#>" + '\n' +
      "SELECT ?o " + '\n' +
      "WHERE {<" + aString + "> owl:sameAs ?o}"
  }

  // Executes a select query over the datasets
  def executeQuery(aQuery: String, aModel: Model): ResultSet = {
    val query = QueryFactory.create(aQuery)
    val qexec = QueryExecutionFactory.create(query, aModel)
    qexec.execSelect()
  }

  // Utility function that returns a String displaying the results of validation
  def showValidity(infModel :InfModel): String = {
    // VALIDITY CHECK against RDFS
    val buf = new StringBuffer()
    val validity = infModel.validate()
    if (validity.isValid) {
      buf.append("The Model is VALID!")
    } else {
      buf.append("Model has CONFLICTS.")
      while (validity.getReports.hasNext) {
        buf.append(" - " + validity.getReports.next() )
      }
    }
    buf.toString
  }

  // Utility function to append the final string to the initial instance types triples file
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B = {
    try { f(param) } finally { param.close() }
  }

  def appendToFile(fileName:String, textData:String) = {
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }
  }

  // We need an update method in case we update models during execution
  def addStatementToModel(aSubject:String, aPredicate:String, anObject:String, aModel:Model) {
    val aSubjectNode = ResourceFactory.createResource(aSubject)
    val anObjectNode = ResourceFactory.createResource(anObject)
    val aStatement = ResourceFactory.createStatement(aSubjectNode, RDF.`type`, anObjectNode)
    aModel.add(aStatement); // add the statement (triple) to the model
  }

  // A method that uses types from Freebase to complement DBpedia resources types remotely, requires a google API key
  def compTypesWithFreebaseRemote(){
    println("Starting types complement using an endpoint of the freebase...")
    // Get the information we need from the companion object
    val mainLanguage = ComplementTypes.mainLanguage(0)
    val freebaseBaseDir = ComplementTypes.freebaseBaseDir
    val outputBaseDir = ComplementTypes.outputBaseDir

    val apiKey = "AIzaSyC7AS70LZ_lAU5NReFneH8ApbEAUEaSmXY"
    val httpTransport = new NetHttpTransport()
    val requestFactory = httpTransport.createRequestFactory()
    val parser = new JSONParser()
    val url = new GenericUrl("https://www.googleapis.com/freebase/v1/search")
    //val url = new GenericUrl("https://www.googleapis.com/freebase/v1/mqlread?")

    // In the case of complementing types using Freebase we only delete the TDB store files to prevent NULL pointer exceptions.
    // This line can be commented out if the model was generated correctly in a previous execution of this method. No statements
    // are added to this model during execution
    //FileUtils.cleanDirectory(new java.io.File(freebaseBaseDir + "/TDB"))

    // Create a model to represent the links between DBpedia and Freebase
    val fbTdbStore = createModel(freebaseBaseDir + "/TDB")
    FileManager.get().readModel( fbTdbStore, freebaseBaseDir + "freebase_links.nt", "N-TRIPLES" )
    println(freebaseBaseDir + "freebase_links.nt")

    val ptEnFileIterator = createNTFileIterator(outputBaseDir + mainLanguage + "/" + mainLanguage + "_en_links.nt")

    while (ptEnFileIterator.hasNext) {
      val stmt = ptEnFileIterator.nextStatement()
      val enObject = stmt.getObject.toString

      val dbFbOccsQuery = buildQueryOWLSameAs(enObject)
      val dbFbOccsResults = executeQuery(dbFbOccsQuery, fbTdbStore)

      while (dbFbOccsResults.hasNext) {

        val soln = dbFbOccsResults.nextSolution()
        val fbObject = soln.get("o").toString.replace("http://rdf.freebase.com/ns/","").replace('.','/')
        //fbObject.replace('.','/')
        println(fbObject)

        //url.put("query", "[{\"id\": /" + fbObject + ",\"type\": []}]")
        //url.put("query", "\"mid\": \"/" + fbObject + "\", \"type\": []")
        url.put("query", "\"mid\": \"/" + fbObject + "\"")
        //url.put("query", fbObject)
        url.put("limit", "20")
        url.put("key", apiKey)

        val request = requestFactory.buildGetRequest(url)
        val httpResponse = request.execute()

        val response = parser.parse(httpResponse.parseAsString())
        //val results = response.get("result").asInstanceOf[JSONArray]
        //val results = response.asInstanceOf[org.json.simple.JSONArray].toString
        val results = response.toString
        //for (result: Object <- results) {
        println(results)
          //println(JsonPath.read(result,"$.name").toString)
        //}
        System.exit(1)
      }
    }
    println("Done!")
  }

  // A method that uses types from Freebase to complement DBpedia resources types locally, requires pre-processing.
  // More information in the freebase_types.sh script
  def compTypesWithFreebaseLocal(){
    println("Starting types complement using a local version of the freebase...")
    // Get the information we need from the companion object
    val mainLanguage = ComplementTypes.mainLanguage(0)
    val instTypesNamesArray = ComplementTypes.instTypesNamesArray
    val freebaseBaseDir = ComplementTypes.freebaseBaseDir
    val outputBaseDir = ComplementTypes.outputBaseDir
    val dbpediaBaseDir = ComplementTypes.dbpediaBaseDir

    // In the case of complementing types using Freebase we only delete the TDB store files to prevent NULL pointer exceptions.
    // These lines can be commented out if the model was generated correctly in a previous execution of this method. No statements
    // are added to these models during execution
    FileUtils.cleanDirectory(new java.io.File(freebaseBaseDir + "/TDB"))
    FileUtils.cleanDirectory(new java.io.File(freebaseBaseDir + "/MID_TDB"))

    // Create a model to represent the links between DBpedia and Freebase
    val fbTdbStore = createModel(freebaseBaseDir + "/TDB")
    FileManager.get().readModel( fbTdbStore, freebaseBaseDir + "freebase_links.nt", "N-TRIPLES" )

    // Create a model to represent a part of the Freebase that we are interested
    val fbMidTdbStore = createModel(freebaseBaseDir + "/MID_TDB")
    FileManager.get().readModel( fbMidTdbStore, freebaseBaseDir + "freebase_mid_types.nt", "N-TRIPLES" )

    val ptEnFileIterator = createNTFileIterator(outputBaseDir + mainLanguage + "/" + mainLanguage + "_en_links.nt")

    while (ptEnFileIterator.hasNext) {
      val stmt = ptEnFileIterator.nextStatement()
      val enObject = stmt.getObject.toString

      val dbFbOccsQuery = buildQueryOWLSameAs(enObject)
      val dbFbOccsResults = executeQuery(dbFbOccsQuery, fbTdbStore)

      while (dbFbOccsResults.hasNext) {
        val soln = dbFbOccsResults.nextSolution()
        val fbObject = soln.get("o").toString.replace("http://rdf.freebase.com/ns/","ns:")

        for (line <- (Source fromFile (freebaseBaseDir + "freebase_ids")).getLines()) {
          // If we have a MID that is guaranteed to link DBpedia with Freebase
          if (line.toString.matches(fbObject)) {
            val fbTypesQuery = buildQueryRDFType(line.toString)
            val fbTypesResults = executeQuery(fbTypesQuery, fbMidTdbStore)

            while (fbTypesResults.hasNext) {
              val soln = fbTypesResults.nextSolution()
              val tmpString = soln.get("o").toString.replace(':','/')
              val aFbType = "http://rdf.freebase.com/" + tmpString.replace('.','/')

              // <subject> <predicate> <object>
              val finalString = "<" + stmt.getSubject.toString + "> <" + RDF.`type` + "> <" + aFbType + "> ."

              // Displays on the screen the final string to be added to the subject
              // instance types triples file. For debugging purposes
              //System.out.println(finalString)

              // We can now concatenate the final string to the current instance types file
              appendToFile(dbpediaBaseDir + mainLanguage + '/' + instTypesNamesArray(0), finalString)
            }
            break()
          }
        }
      }
    }

    // Close the datasets now that we are done
    fbTdbStore.close()
    fbMidTdbStore.close()
    println("Done!")
  }

  // A method to complement types of one language using others. All the information is defined in the indexing.properties file
  // and is loaded in the companion object
  def compTypesWithOtherLanguages() {
    println("Starting types complement using other languages...")
    // Get the information we need from the companion object
    val mainLanguage = ComplementTypes.mainLanguage(0)
    val allLangsArray = ComplementTypes.allLangsArray
    val instTypesNamesArray = ComplementTypes.instTypesNamesArray
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
      instTypesNamesArray(i) = "instance_types_" + langName + ".nt"
      println(instTypesNamesArray(i))
      println(tdbStoreBaseDir + langName + "/TDB")
      println(dbpediaBaseDir + langName + '/' + instTypesNamesArray(i))
      tdbStoreArray(i) = createModel(tdbStoreBaseDir + langName + "/TDB")
      FileManager.get().readModel( tdbStoreArray(i), dbpediaBaseDir + langName + '/' + instTypesNamesArray(i), "N-TRIPLES" )
      if (i > 0) {
        println(outputBaseDir + mainLanguage + '/' + allLangsArray(0) + '_' + langName + '_' + "links.nt")
        fileIteratorArray(i-1) = createNTFileIterator(outputBaseDir + mainLanguage + '/' + allLangsArray(0) + '_' + langName + '_' + "links.nt")
      }
      i += 1
    }

    // Now for each bijective inter languages links file we proceed with the core routine of the language types complement.
    // First it executes the query over the main language instance types file to see if there are types for the current resource.
    // If not, executes another query using the sameAs relation in the complement language instance types file. In the end the
    // routine adds the results to the current instance types file of the main language, complementing it
    i = 1
    for( aLinksFile <- fileIteratorArray ){
      while (aLinksFile.hasNext) {
        val stmt = aLinksFile.nextStatement()
        val subject = stmt.getSubject.toString
        val obj = stmt.getObject.toString

        // The idea is to query the instance types file related to the object in order to complement the instance types file
        // related to the subject. For instance, if in portuguese a subject has no types associated to it, we can
        // use other languages to find dbpedia types for it
        val newSubject = obj

        // A query to find if the subject from the main language has any types in the instance types triples file
        val occsQuery = buildQueryRDFType(subject)
        val occsResults = executeQuery(occsQuery, tdbStoreArray(0))

        // A query to find the types of a subject in the instance types triples file
        val newTypesQuery = buildQueryRDFType(newSubject)
        val newTypesResult = executeQuery(newTypesQuery, tdbStoreArray(i))

        //TODO, if the subject has at least one type, search for new types in the object instance types file and add them accordingly, a join operation
        /*if (occsResults.hasNext()) {
          while (occsResults.hasNext()) {

          }
        } else {*/
        if (!occsResults.hasNext) {

          // Make a pattern so we can replace the owl occurrences
          val pattern = new Regex("(O|o)wl")

          // Iterating the ResultSet to get all its elements
          while (newTypesResult.hasNext) {
            // <subject> <predicate> <object>
            var finalString = "<" + subject + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
            val soln = newTypesResult.nextSolution()
            var newObject = ""

            if ((pattern findFirstMatchIn soln.toString) != None) {
              newObject = "http://www.w3.org/2002/07/owl#Thing"
            } else {
              newObject = soln.get("o").toString
            }
            finalString = finalString + "<" + newObject + "> ."

            // Displays on the screen the final string to be added to the subject
            // instance types triples file. For debugging purposes
            //System.out.println(finalString)

            // We can now concatenate the final string to the current instance types file and update the
            // current model
            appendToFile(dbpediaBaseDir + mainLanguage + '/' + instTypesNamesArray(0), finalString)
            addStatementToModel(subject, RDF.`type`.toString, newObject, tdbStoreArray(0))
          }
        }
      }
      i += 1
    }

    // Close the datasets now that we are done
    i = 0
    for( langName <- allLangsArray ){
      tdbStoreArray(i).close()
      i += 1
    }
    println("Done!")
  }
}

object ComplementTypes {
  private val LOG = LogFactory.getLog(this.getClass)
  val aTypeManager = new ComplementTypes()

  // Creates an empty property list
  val config: Properties = new Properties()

  try {
    config.load(new FileInputStream(new java.io.File("conf/indexing.properties")))
  }
  catch {
    case e: IOException => {
      throw new ConfigurationException("Cannot find configuration file " + "conf/indexing.properties", e)
    }
  }

  // Read in from the indexing.properties files all the data we need. Creates arrays accordingly
  val mainLanguage = Array(config.getProperty("org.dbpedia.spotlight.language_i18n_code", ""))
  val compLangsArray = config.getProperty("org.dbpedia.spotlight.complement_languages", "").split(",").toArray
  val allLangsArray = mainLanguage ++ compLangsArray

  // Checks if the number of languages is not valid
  aTypeManager.testArrayLength(allLangsArray.length, LOG)

  // Get the base directories used in this process. The user can set the paths to them in the indexing.properties file.
  // If the download.sh script was executed with the complement types option, all the needed directories were already created
  // and are defined inside the script.
  // TODO: make the download.sh script use directories arguments from the indexing.properties file, centralizing this process. ALso change the comment above accordingly
  val tdbStoreBaseDir = config.getProperty("org.dbpedia.spotlight.data.tdbStoreBaseDir","")
  val dbpediaBaseDir = config.getProperty("org.dbpedia.spotlight.data.dbpediaBaseDir","")
  val outputBaseDir = config.getProperty("org.dbpedia.spotlight.data.outputBaseDir","")
  val freebaseBaseDir = config.getProperty("org.dbpedia.spotlight.data.freebaseBaseDir","")

  // Creates an array to hold all the names of the instance types files we are going to need
  val instTypesNamesArray = new Array[String](allLangsArray.length)
  // Creates an array to hold all models so we can query the instance types files from all the required languages
  val tdbStoreArray = new Array[Model](allLangsArray.length)
  // Creates an iterator array so we can check every resource of the main language related to each complement language
  val fileIteratorArray = new Array[StmtIterator](allLangsArray.length-1)

  // Checks if the number of files to iterate through is not valid
  aTypeManager.testArrayLength(fileIteratorArray.length, LOG)

  def main(args : Array[String]) {
    // Core methods for the types complement task
    //aTypeManager.compTypesWithOtherLanguages
    //aTypeManager.compTypesWithFreebaseLocal()
    aTypeManager.compTypesWithFreebaseRemote()
  }
}