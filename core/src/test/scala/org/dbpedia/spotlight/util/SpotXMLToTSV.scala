/*
 * Copyright 2011 Pablo Mendes, Max Jakob
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

import org.dbpedia.spotlight.spot.SpotXmlParser
import org.dbpedia.spotlight.model.Text

/**
 * Created with IntelliJ IDEA.
 * User: Renan
 * Date: 02/07/13
 * Time: 09:43
 * To change this template use File | Settings | File Templates.
 */

class SpotXMLToTSV(fromFormat: String, toFormat: String, aSpotter: SpotXmlParser) {


}

object SpotXMLToTSV {
  def main(args: Array[String]) {
    val xml = "<annotation text=\"The research, which is published online May 22 in the European Heart Journal, opens up the prospect of treating heart failure patients with their own, human-induced pluripotent stem cells (hiPSCs) to repair their damaged hearts.\">\n<surfaceForm name=\"published\" offset=\"23\"/>\n<surfaceForm name=\"May 22\" offset=\"40\"/>\n<surfaceForm name=\"European\" offset=\"54\"/>\n<surfaceForm name=\"Heart\" offset=\"63\"/>\n<surfaceForm name=\"Journal\" offset=\"69\"/>\n<surfaceForm name=\"prospect\" offset=\"91\"/>\n<surfaceForm name=\"heart failure\" offset=\"112\"/>\n<surfaceForm name=\"patients\" offset=\"126\"/>\n<surfaceForm name=\"human\" offset=\"151\"/>\n<surfaceForm name=\"stem cells\" offset=\"177\"/>\n<surfaceForm name=\"hearts\" offset=\"221\"/>\n</annotation>"
    val spotter = new SpotXmlParser()
    //spotter.extract(new Text(xml)).foreach(println)
    spotter.extract(new Text(xml))

    //if (spotter.occurences.isEmpty) sys.exit(1)

    //spotter.occurences.foreach(println)

    val aConverter  = new SpotXMLToTSV("XML", "TSV", spotter)
  }
}
