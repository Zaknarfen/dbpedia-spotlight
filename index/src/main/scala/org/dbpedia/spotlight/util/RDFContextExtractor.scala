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

import java.io.InputStream
import java.io.PrintStream
import scala.Option.option2Iterable
import com.hp.hpl.jena.rdf.model.Model
import com.typesafe.config.{ConfigFactory, Config}

/**
 * Context Extraction from RDF
 * Used to process the properties and labels and group them by the subject
 * The output is formatted as TSV or JSON
 *
 * @author reinaldo
 */
object RDFContextExtractor extends App {

  // val config = ConfigFactory.load;

  /**
   * Choose the format of the output
   * Choose the relevant part to be extracted: object, property or type
   * Fill the input and output file names
   * Check the output to see the extraction
   */

  /** Use it to create databases at filesystem*/
  /** DataConn.create_TDB_Filesystem(
    * config.getString("dataSet.location"),
    * config.getString("dataSet.inputFile"));*/


  /** Get file extension */
  def extensor(file: String) = file.split('.').drop(1).lastOption

  /** Loads a datafile into the TDB DataConn.dataset **/
  def loadDataSet(tdbFile: String, tdbLocation: String, tdbFormat: String) {
    /** Convert TDB input file into InputStream*/
    val tdbModel: InputStream = //if (extensor(tdbFile).equals(Some("bz2")))
      BZipUntar.convert(tdbFile)
    //else BZipUntar.toInputStream(tdbFile)
    /** Creates and populates TDB */
    DataConn.createTDBFilesystem(tdbLocation, tdbModel, tdbFormat)
  }

  /**
   * Extractor for windows and unix. It takes the dataset files to extract them.
   * args:
   *
   * - loadtdb: represents whether it is necessary to reload the main tdb: DataConn.dataSet
   * - tdbFile: main tdb input File
   * - tdbLocation: main tdb location
   * - tdbFormat: main tbb format, default (config file) is "RDF/XML"
   * - modelFile: model input file
   * - modelFormat: model format, default is "N-TRIPLE"
   * - extraction: extraction part - object or property.
   * - outformat: output format - TSV or JSON
   * - outputFile: result file
   * - namedModel: model name
   */
  def extract(loadtdb: Boolean,
              tdbFile: String,
              tdbLocation: String,
              tdbFormat: String,
              modelFile:String,
              modelFormat:String,
              extraction: String,
              outformat: String,
              outputFile: String,
              namedModel: String) {

    /** reload the DataConn.dataSet TDB if requested */
    if (loadtdb)
      loadDataSet(tdbFile, tdbLocation, tdbFormat)

    /** converts Model input file into InputStream**/
    val input: InputStream = ///if (extensor(modelFile).equals(Some("bz2")))
      BZipUntar.convert(modelFile)
    // else BZipUntar.toInputStream(modelFile)

    /** Get the main TDB */
    DataConn.getTDBFilesystem(tdbLocation)

    /** Add Model into main TDB as a namedModel **/
    DataConn.addModelToTDB(namedModel, input, modelFormat)

    /**Execute the extraction itself */
    RDFContextExtractor.labelExtraction(
      extraction,
      outformat,
      DataConn.dataSet.getNamedModel(namedModel),
      outputFile)

    /** Close tdb **/
    DataConn.dataSet.close()
  }

  def extractPropertiesJSON( reloadDeafaultModel: Boolean, modelFile:String, outputFile: String, namedModel: String, tdbOutputDir: String, tdbInputDatasetDir: String) {
    /** get config file **/
    val config = ConfigFactory.load

    /*println(config.getString("dataSet.inputFile"))
println(config.getString("dataSet.location"))
println(config.getString("dataSet.format"))
println(config.getString("execution.inputFormat"))
println(config.getString("execution.extraction"))
println(config.getString("execution.outputFormat"))*/

    extract2(reloadDeafaultModel, //If it is necessary to load the default model. If is has already been loaded, pass false.
      config.getString("dataSet.inputFile") ,
      config.getString("dataSet.location") + "/labels",
      config.getString("dataSet.format"),
      modelFile,
      config.getString("execution.inputFormat"),
      config.getString("execution.extraction"),
      config.getString("execution.outputFormat"),
      outputFile,
      tdbOutputDir + "/" + namedModel,
      tdbInputDatasetDir)

  }

  /**
   * Extractor for *Unix only*. It takes the dataset files to extract them.
   * args:
   *
   * - loadtdb: represents whether it is necessary to reload the main tdb: DataConn.dataSet
   * - tdbFile: main tdb input File - usually labels.nt
   * - tdbLocation: main tdb location - labels.nt
   * - tdbFormat: main tbb format, default (config file) is "RDF/XML" - labels.nt
   * - modelFile: model input file
   * - modelFormat: model format, default is "N-TRIPLE"
   * - extraction: extraction part - object or property.
   * - outformat: output format - TSV or JSON
   * - outputFile: result file
   * - namedModelLocation: model tdb location
   * - datasetsLocation: directory when the downloaded dataset will be stored
   */
  def extract2(loadtdb: Boolean,
               tdbFile: String,
               tdbLocation: String,
               tdbFormat: String,
               modelFile:String,
               modelFormat:String,
               extraction: String,
               outformat: String,
               outputFile: String,
               namedModelLocation: String,
               datasetsLocation: String) {

    /** reload the DataConn.dataSet TDB if requested */
    if (loadtdb) {
      println("Loading Main TDB")
      loadDataSet(tdbFile, tdbLocation, tdbFormat)
    }

    /** Get the main TDB */
    DataConn.getTDBFilesystem(tdbLocation)

    println("Loading " + modelFile + "TDBloader2")

    /*** Load TDB by using tdbloader2 - for Linux only **/
    LinuxTDBLoader.tdbloader2(modelFile, datasetsLocation, namedModelLocation)

    println("Getting Model " + namedModelLocation)
    /** Add Model into separated TDBs **/
    val model: Model = DataConn.getTDBFilesystemDataset(namedModelLocation).getDefaultModel

    println("Extracting")
    /**Execute the extraction itself */
    RDFContextExtractor.labelExtraction(
      extraction,
      outformat,
      model,
      outputFile)

    /** close tdbs used **/
    model.close()
    DataConn.dataSet.close()
  }

  /**
   * Reads in files into a Jena Model.
   * Performs context extraction, formatting and outputs context into a file.
   * Context extraction can focus on property labels, object labels or object type labels.
   */
  def labelExtraction(partToBeExtracted: String, format: String, namedModel: Model, outputFile: String) {

    /** creating the output*/
    val output = new PrintStream(outputFile)

    /** choosing output format*/
    val formatter = if (format.equals("TSV")) new TSVOutputFormatter else new PigOutputFormatter

    /** choosing extraction strategy*/
    val extractor = if (partToBeExtracted.equals("object")) new ObjectExtractor else new PropertyExtractor

    /** applying over input*/
    var source: JenaStatementSource = new JenaStatementSource( namedModel)

    source.groupBy(e => e.getSubject).flatMap {
      case (subject, statements) => {
        val context = extractor.extract(statements).mkString(" ")
        output.println(formatter.format(subject.getLocalName, context))
        Some((subject, context))
      }
      case _ => None
    }.toSeq
      .sortBy(e => e._1.getLocalName)

    output.close()
    source = null
  }
}
