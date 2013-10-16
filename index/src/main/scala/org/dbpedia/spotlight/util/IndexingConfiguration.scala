/*
* Copyright 2012 DBpedia Spotlight Development Team
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
* Check our project website for information on how to acknowledge the authors and how to contribute to the project: http://spotlight.dbpedia.org
*/

package org.dbpedia.spotlight.util

import scala.collection.JavaConversions._
import org.apache.lucene.analysis.Analyzer
import org.dbpedia.spotlight.exceptions.ConfigurationException
import java.io._
import org.dbpedia.spotlight.model.Factory

/**
 * Class that holds configuration values for indexing tasks.
 *
 * @author maxjakob
 * @author pablomendes (added getters, multi-language support)
 */

class IndexingConfiguration(aConfigFilePath: String = "conf/indexing.properties") extends ConfigurationLoader(aConfigFilePath) {

  validate()

  def getAnalyzer : Analyzer = {
    val lang = get("org.dbpedia.spotlight.language")
    Factory.Analyzer.from(get("org.dbpedia.spotlight.lucene.analyzer"),get("org.dbpedia.spotlight.lucene.version"), getStopWords(lang))
  }

  private def validate() { //TODO move validation to finer grained factory classes that have specific purposes (e.g. candidate mapping, lucene indexing, etc.)

    val language = get("org.dbpedia.spotlight.language")
    if(language==null || language.size==0) {
      throw new ConfigurationException("Parameter org.dbpedia.spotlight.language not specified in config")
    }

    val stopwordsFile = new File(get("org.dbpedia.spotlight.data.stopWords"))
    if(!stopwordsFile.isFile) {
      throw new ConfigurationException("specified stop words file not found: "+stopwordsFile)
    }

    val analyzerName = get("org.dbpedia.spotlight.lucene.analyzer")
    if(analyzerName==null) throw new ConfigurationException("Analyzer not specified")

  }
}