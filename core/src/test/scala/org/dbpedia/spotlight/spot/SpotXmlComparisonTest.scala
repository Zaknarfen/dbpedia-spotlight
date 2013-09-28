package org.dbpedia.spotlight.spot

//import scala.io.Source._

import xml.{Node, XML}
import org.dbpedia.spotlight.model.{SurfaceFormOccurrence, SurfaceForm, Text}
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: Renan
 * Date: 01/05/13
 * Time: 13:45
 * To change this template use File | Settings | File Templates.
 */
class SpotXmlComparisonTest extends Spotter { //extends FlatSpec with ShouldMatchers {
  var name = "SpotXmlComparisonTest"

  /**
   * Extracts a set of surface form occurrences from the text
   */
  def extract(spotsXml: Text): java.util.List[SurfaceFormOccurrence] = {
    val xml = XML.loadString(spotsXml.text)
    val text = (xml \\ "annotation" \ "@text").toString
    val surfaceForms = xml \\"annotation" \ "surfaceForm"
    val occs = surfaceForms.map(buildOcc(_, new Text(text)))

    //Scala conversion to a list structure
    occs.toList
  }

  def buildOcc(sf: Node, text: Text) = {
    val offset = (sf \ "@offset").toString.toInt
    val name = (sf \ "@name").toString
    new SurfaceFormOccurrence(new SurfaceForm(name), text, offset)
  }

  def getName() = name

  def setName(n: String) {
    name = n;
  }
}

object SpotXmlComparisonTest {
  def main(args: Array[String]) {
    //Opens the first file, the hand annotation XML
    val handXML = scala.io.Source.fromFile("C:\\Users\\Renan\\Documents\\GitHub\\dbpedia-spotlight\\core\\src\\test\\resources\\annotation1.xml", "UTF-8").mkString

    //Opens the second XML file, generated by the client
    val autoXML = scala.io.Source.fromFile("C:\\Users\\Renan\\Documents\\GitHub\\dbpedia-spotlight\\core\\src\\test\\resources\\candidates1.xml", "UTF-8").mkString

    //Instantiate the object we are going to need
    val spotter = new SpotXmlComparisonTest()

    //Builds both lists we will need to compare
    val handList = spotter.extract(new Text(handXML))
    val autoList = spotter.extract(new Text(autoXML))

    //Filtering the manual annotation list using the client generated list
    val nDetectedList = handList.filterNot(autoList.contains(_))

    //Getting the manual annotation list size and the not detected annotation list size to calculate the client accuracy
    val handListLength:Double = handList.length
    val nDetListLength:Double = nDetectedList.length

    //Finally calculate the accuracy of the client
    val accuracy:Double = (((handListLength - nDetListLength)/handListLength)*100.0f)
    printf("The accuracy of the client is = %.2f", accuracy)
  }
}