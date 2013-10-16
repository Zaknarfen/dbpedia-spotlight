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

import com.hp.hpl.jena.rdf.model.{InfModel, ModelFactory, StmtIterator, Model}
import com.hp.hpl.jena.tdb.TDBFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.query.{QueryExecutionFactory, QueryFactory, ResultSet}

object TDBHelper {
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
  def buildQueryRDFLabel(aString: String): String = {
    "PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>" + '\n' +
      "SELECT ?o " + '\n' +
      "WHERE {<" + aString + "> rdf:label ?o}"
  }

  def buildQueryRDFType(aString: String): String = {
    if (aString.contains("http")) {
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + '\n' +
        "SELECT ?o " + '\n' +
        "WHERE {<" + aString + "> rdf:type ?o}"
    } else {
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + '\n' +
        "SELECT ?o " + '\n' +
        "WHERE {<" + aString.replaceAll("ttp", "http") + "> rdf:type ?o}"
    }
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
}