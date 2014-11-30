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

package com.twitter.scadual.tsql

import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scaldual.tutorial.LocalTableFunction
import org.specs._
import org.specs.mock.Mockito

object TestData {
  val path = "src/main/resources/tutorial/docBOW.tsv"
  val input = Tsv(path, fields = new Fields("A", "B", "C"))
  val data = List((1, "The", 5), (2, "Dog", 12), (3, "Car", 15))
  val inputLine = TypedPsv[String](path)
  val dataLine = List("1\tThe\t5", "2\tDog\t12", "3\tCar\t15")
}
class TsqlTest extends Specification with Mockito {
  import TestData._
  "A TSQL Class" should {
    JobTest(new com.twitter.scaldual.tsql.TSqlJob(_)).
      arg("output", "outputFile").
      arg("input", path).
      arg("query", """select A, B from FILE""").
      source(input, data).
      source(inputLine, dataLine).
      sink[(String, String)](Tsv("outputFile")){ outputBuffer =>
        "correcly output data" in {
          outputBuffer.size must beEqualTo(3)
        }
      }.
      run.
      finish
  }
}