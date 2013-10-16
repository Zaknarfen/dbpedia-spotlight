package org.dbpedia.spotlight.util

import java.io._
import java.util.Properties
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.exceptions.ConfigurationException
import scala.io.Source

class ConfigurationLoader(private val aConfigFilePath: String = "") {

  val configFilePath = aConfigFilePath
  val configFile: File = new java.io.File(configFilePath).getAbsoluteFile
  if (configFile == null) {
    throw new Exception("Invalid file path! "+configFilePath)
  }
  val properties: Properties = new Properties()
  loadConfiguration(properties)

  def loadConfigurationFile(x: Properties) = {
    val aInputStream = new FileInputStream(configFile)
    if (aInputStream == null) {
      throw new Exception("Cannot load the configuration file!")
    }
    x.load(aInputStream)
    println("Loaded configuration file " + configFilePath + """.""")
  }

  def loadConfiguration(aConfig: Properties) = {
    aConfig match {
      case x: Properties => {
        loadConfigurationFile(aConfig)
      }
      case _ => {
        throw new Exception("Cannot load the configuration file!")
      }
    }
  }

  def save(configFile: File) {
    properties.store(new FileOutputStream(configFile), "")
    println("Saved configuration file " + configFile + """.""")
  }

  def save() {
    save(configFile)
  }

  def get(key : String, defaultValue : String) : String = {
    properties.getProperty(key, defaultValue)
  }

  def get(key : String) : String = {
    val value = get(key, null)
    if(value == null) {
      throw new ConfigurationException(key+" not specified in " + configFile + """.""")
    }
    value
  }

  def set(key : String, newValue : String) {
    val value = get(key, null)
    if (value == null) {
      throw new ConfigurationException(key+" not specified in " + configFile + """.""")
    }
    properties.setProperty(key, newValue)
  }

  def getStopWords(language: String) : Set[String] = {
    val f = new File(get("org.dbpedia.spotlight.data.stopWords", ""))
    try {
      Source.fromFile(f, "UTF-8").getLines().toSet
    }
    catch {
      case e: FileNotFoundException => throw new ConfigurationException("Stop words file not found: "+ f + """.""", e)
    }
  }

  def getLanguage = {
    get("org.dbpedia.spotlight.language")
  }
}