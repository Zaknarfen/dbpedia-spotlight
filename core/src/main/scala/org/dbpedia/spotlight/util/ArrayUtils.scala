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

package org.dbpedia.spotlight.util

import org.apache.commons.logging.Log

object ArrayUtils {
  def testArrayLength(requiredLength: Int, anArrayLength: Int, aLog: Log) {
    if (anArrayLength < 1) {
      aLog.error("At least one language must be supplied to execute this process.")
      System.exit(1)
    }
  }
}
