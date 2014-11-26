/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding

import com.twitter.scaldual.tutorial.LocalTableFunction
import cascading.tuple.Fields
import org.specs._
import org.specs.mock.Mockito
import cascading.flow.FlowDef

object TestData {
  val path = "src/main/resources/tutorial/docBOW.tsv"
  val input = Tsv(path, fields = new Fields("doc_id", "word", "count"))
  val data = List((1, "The", 5), (2, "Dog", 12), (3, "Car", 15))
}
class LingualTutorialTest extends Specification with Mockito { 
  implicit val flowDef = mock[FlowDef]
   
  import TestData._
  "A Simple Tutorial" should {     
    JobTest("com.twitter.scaldual.tutorial.LingualLocal").
      arg("output", "outputFile").
      source(input, data).
      sink[(String, Int)](Tsv("outputFile")){ outputBuffer =>
        "correcly output data" in {
          outputBuffer.size must beEqualTo(3)
        }
      }.
      run.
      finish
  }
  
  "A Tabled Tutorial" should {     
    val table = LocalTableFunction()
    JobTest("com.twitter.scaldual.tutorial.LingualTableLocalFunction").
      arg("output", "outputFile").
      source(table.source, data).
      source(table.tmpFile, data.map(table.function(_))).
      sink[(String, Int)](Tsv("outputFile")){ outputBuffer =>
        "correcly output data" in {
          outputBuffer.size must be_>=(0)
        }
      }.
      run.
      finish
  }
}