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
import java.net.URL

import scala.io.Source
import scala.sys.process.ProcessLogger
import scala.sys.process.stringSeqToProcess

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import com.hp.hpl.jena.util.FileManager

/**
 * A conventional way to read a http .bz2 file and convert it to InputStream.
 * It may be used for both Windows and Unix environments when dealing with small datasets.
 *
 * The BZipUntar reads a *.bz2 file and return its associated InputStream.
 * to that end, call convert method.
 *
 * TODO: 1. May need some changes to handle exceptions.
 *                  2. Use other compression files
 *
 * @author Carol Valadares
 *
 */
object BZipUntar {

  /**
   * Converts an InputString to String. Do not Use it for large files
   */
  //def inputStreamToString(is: InputStream): String = {
  // Source.fromInputStream(is).getLines.reduceLeft(_ + _)
  //}

  /**
   * Gets an compressed URL (Http) or local file (:file) - as a*.bz2 file -
   * and converts it to InputStream.
   */
  def convert(path: String): InputStream = {
    val url: URL = new URL(path)
    unzipped(url)
  }

  /**
   * Open a local and uncompressed file.
   */
  def toInputStream(path: String): InputStream = {
    FileManager.get.open(path)
  }

  /**
   * Performs the unzipping itself.
   */
  private def unzipped(url: URL): InputStream = {
    new BZip2CompressorInputStream(url.openStream(), true)
  }

}

/**
 * ** For Unix environemnt only **
 *
 * LinuxTDBLoader Loads an input dataset into a TDB Model.
 * Bear in mind that it loads a new TDB every time it is run.
 * There is no way to load multiples Named Model in this version.
 *
 * ----- For Unix only -----------------------------------------------------------
 * -- Requeriments:                                                                                                                                --
 * -- 1) Install TDB jena package                                                                                                --
 * -- 2) add it into your classpath                                                                                                --
 * -- Further details: http://jena.apache.org/documentation/tdb/commands.html        --
 * -------------------------------------------------------------------------------
 *
 * The tdbloader2 Scala representation. Advisable for large datasets.
 *
 * It executes the following steps (by using jena.org ):
 *
 * 1) It takes a compressed link (*.bz2 only for now);
 * 2) download it to a specific directory (datasetLocation);
 * 3) uncompress it into the same directory (datasetLocation); and
 * 4) calls tdbLoad2 command to load a new TDB file in tdbLocation directory.
 *
 * TODO: 1) reads other files rather than only bz2.
 *
 * @author Carol Valadares
 *
 */
object LinuxTDBLoader {

  /** Get a file extension without dot ([.]ext) */
  def extensor(file: String) = file.split('.').drop(1).lastOption

  /**
   * tdbLoad2 unix representation
   * arguments:
   * - httpLink: a bz2 file (http)
   * - datasetLocation: location in which the dataset will be stored and uncompressed
   * - tdbLocation: The TDB location
   */
  def tdbloader2(httpLink: String, datasetLocation : String, tdbLocation: String) : Array[String] = {


    /** download http file into datasetLocation directory **/
    val bz2File:String = wget(datasetLocation, httpLink)
    /** uncompress the downloaded file in the same directory **/
    val unzipedFile:String = unzip(bz2File)
    /** tdbloader2 representation by taking an uncompressed dataset and loading it into tdbLocation directory **/
    tdbLoad2(tdbLocation, unzipedFile)
    /** Return the dataset names and location */
    Array(unzipedFile, tdbLocation + "/" + unzipedFile)
  }

  def fileExists(file: String) {
    val command: Array[String] = Array("!", "-f", file.trim )
    println(command)
    //process(command)
  }

  /***
    * A generic Linux process representation.
    * It takes as parameters an array of command lines and execute them.
    */
  def process(args: Array[String]) {
    /** log file */
    var lines = Vector.empty[ String ]
    val log = ProcessLogger( lines :+= _ )
    /** Execute command line and logs its execution */
    println(stringSeqToProcess(args))
    val process = stringSeqToProcess (args).run( log )
    /** Blocks until process is finished and returns the exit value **/
    val response = process.exitValue()
    /** The resulting output **/
    println(lines.mkString( "\n" ))
  }

  /***
    * wget representation to download http files.
    * usage:
    *
    * wget -P destinationDirectory linkTobeDownloaded
    */
  def wget(path: String, link: String) : String = {
    /** Download file **/
    val command: Array[String] = Array( "wget", "-P" , path, link)
    process(command)

    /** File name **/
    path + "/" + link.split("/").last
  }

  /**
   * bzip2 representation to untar .bz2 files in the current directory.
   * usage:
   *
   * bzip2 -d file
   */
  def unzip(file: String) : String = {
    /** Uncompress file **/
    val command: Array[String] = Array("bzip2", "-d" , file)
    process(command)

    /**File name**/
    file.replace(".bz2", "")
  }

  /***
    * tdbloader2 representation to load a dataset file into a new TDB Model.
    * usage:
    *
    * tdbloader2 --loc tdbLocation datasetInputfile
    */
  def tdbLoad2(location: String, datafile: String) {
    /** Load TDB **/
    val command: Array[String] = Array("tdbloader2", "--loc" , location, datafile)
    process(command)
  }
}
