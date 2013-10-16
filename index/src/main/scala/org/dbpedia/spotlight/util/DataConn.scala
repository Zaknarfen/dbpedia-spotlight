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
*/

package org.dbpedia.spotlight.util

import java.io.IOException
import java.io.InputStream

import org.apache.jena.iri.impl.IRIImplException
import org.apache.jena.riot.RiotException

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.ParameterizedSparqlString
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFReader
import com.hp.hpl.jena.tdb.TDBFactory


/**
 * Object to create and to load a database connection
 * It provides a database connection (variable dataSet) that is used in the whole project to execute queries
 *
 * @author reinaldo
 */
object DataConn {

  /** TDB, in which the DefaultModel holds Labels model and
    * NamedModel holds either properties or types models
    */
  var dataSet: Dataset = null

  /**
   * Creates a Jena dataset populating both Labels and (Properties/Types) Models
   */
  def createTDB(
                 labelData: String,
                 labelFormat: String,
                 labelInput: InputStream,
                 modelName: String,
                 modelInput: InputStream,
                 modelFormat: String) = {

    /** Creates dataSet*/
    createTDBFilesystem(labelData, labelInput, labelFormat)
    /** Adds a named Model into dataSet*/
    addModelToTDB(modelName, modelInput, modelFormat)
  }

  /**
   * Creates a Jena Dataset, and adds a Labels Model from InputStream file
   */
  def createTDBFilesystem(outputData: String, is: InputStream, format: String) = {
    /** Creates tdb */
    dataSet = TDBFactory.createDataset(outputData)
    /** Creates default Model and populate it for labels dataset */
    val tdb: Model = readModel(is, format) //dataSet.getDefaultModel
    tdb.close()
  }

  /**
   * Adds a new NamedModel into dataSet.
   * It might be a properties or a type dataset.
   */
  def addModelToTDB(modelName: String, is: InputStream, format: String) = {
    /** Creates a Model for Properties or Types dataset and populate it */
    val model2: Model = readModel(is, format)//ModelFactory.createDefaultModel//TDBFactory.createModel()
    /** Adding model into dataset*/
    dataSet.addNamedModel(modelName, model2)
  }

  /****
    * Reads InputStrem into Model by using RDFReader.
    * Better for medium files as it only uses available memory RAM.
    */
  def readModel (is: InputStream, format: String) : Model = {
    /** Creates a model*/
    val model: Model = ModelFactory.createDefaultModel
    /** Reads the input file into model **/
    val reader = model.getReader(format)
    reader.setProperty("allowBadURIs", "true")
    reader.setProperty("relativeURIs", "")
    reader.setProperty("tab", "0")
    reader.setProperty("WARN_REDEFINITION_OF_ID", "EM_IGNORE")
    try{
      reader.read(model, is, null)
    } catch {
      case (e: RiotException) => {
        /** Catches some cases in which the dataset has bad URIs**/
        println("Error 05: java.io.IOException: unexpected end of stream")
        println(e.getStackTrace)
      }
    }

    /** Return the model just loaded */
    model
  }

  /**
   * Traditional reading of InputStrem to Model.
   * For small datasets.
   */
  def readIntoModel(model: Model, is: InputStream, format: String): Model = {
    /** Reading inputStrem file with a specific format */
    if (is != null) {
      model.read(is, null, format)
      is.close()
    } else {
      throw new IOException("Cannot open input file %s") //.format(input))
    }
    model
  }

  /**
   * Creates a Jena Dataset, and adds a Labels Model from a local file
   * CV: Old method.
   *
  def createTDBFilesystem(outputData: String, inputFile: String) = {
dataSet = TDBFactory.createDataset(outputData)
val tdb: Model = dataSet.getDefaultModel
FileManager.get.readModel(tdb, inputFile)
tdb.close
}
   *
   */

  /**
   * Grab tdb file
   */
  def getTDBFilesystem(outputData: String) = {
    dataSet = TDBFactory.createDataset(outputData)
  }

  /***
    * Grab a temporary tdb file - used for tdbloader2 method on ExtractData.
    */
  def getTDBFilesystemDataset(outputData: String) : Dataset = {
    TDBFactory.createDataset(outputData)
  }

  /**
   * Query data from Labels Model
   */
  def executeQuery(uri: String): String = {
    val queryStr: ParameterizedSparqlString = new ParameterizedSparqlString("SELECT * WHERE { ?s ?p ?o }")
    queryStr.setIri("s", uri)
    queryStr.setIri("p", "http://www.w3.org/2000/01/rdf-schema#label")
    var query: Query = null
    var result: String = null

    /**
     * Carol: To avoid the whole program to stop extracting from database,
     * we catch those cases in which there is a data corruption.
     */
    try{
      query = QueryFactory.create(queryStr.toString)
      val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataSet)
      val results: ResultSet = queryExec.execSelect

      if (results.hasNext) {
        val querySolution = results.next
        result = querySolution.getLiteral("o").getString
      }
    }
    catch {
      case e: IRIImplException => {
        println("Error 01: IRIImplException at URI " + uri )
        println(e.toString)
        //println(queryStr.toString);
        //throw new RuntimeException(e)
      }
      case e: com.hp.hpl.jena.query.QueryParseException => {
        println("Error 02: com.hp.hpl.jena.query.QueryParseException at URI " + uri )
        println(e.toString)
        //println(queryStr.toString);
        //throw new RuntimeException(e)
      }
    }
    result
  }
}
