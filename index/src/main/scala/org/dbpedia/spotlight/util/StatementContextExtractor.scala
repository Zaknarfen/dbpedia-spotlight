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

import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFVisitor
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Statement

/**
 * Class that extracts different context definitions from a Jena Statement.
 */
trait StatementContextExtractor {
  def extract(property: Statement) : String
  def extract(statements: Traversable[Statement]) : Traversable[String] = {
    statements.map(s => extract(s))
  }
}

class ObjectExtractor extends StatementContextExtractor {
  def extract(property: Statement) : String = {
    property.getObject.visitWith(new StatementVisitor).toString
  }
}

class PropertyExtractor extends StatementContextExtractor {
  def extract(property: Statement) : String = {
    property.getPredicate.visitWith(new StatementVisitor).toString
    //property.getPredicate.getLocalName
  }
}

/**
 * Visitor used to get the label according to the object type
 */
class StatementVisitor extends RDFVisitor {

  def visitBlank(node: Resource, arg1: AnonId): AnyRef = {
    ""
  }

  def visitLiteral(literal: Literal): AnyRef = {
    literal.getLexicalForm
  }

  def visitURI(resource: Resource, uri: String): AnyRef = {
    val result = DataConn.executeQuery(uri)
    if (result == null) resource.getLocalName else result
  }
}