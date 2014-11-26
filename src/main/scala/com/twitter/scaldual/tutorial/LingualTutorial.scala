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

package com.twitter.scaldual.tutorial

import cascading.tuple.Fields
import cascading.flow.{Flow, FlowDef}
import com.twitter.scalding._
import com.twitter.scaldual.core._

/*
* Very simple Lingual Job that outputs two columns from the phones data file
* Gives a simple example of how the job is setup and how to write a query
*
* To run you must use the lingual assembled jar
* ./scripts/scald.rb \
*   --local com.twitter.scaldual.tutorial.LingualTableLocal \
*   --output /tmp/lingual_test.tsv
*/
class LingualLocal(args:Args) extends LingualJob(args){
  table("bow", Tsv("src/main/resources/tutorial/docBOW.tsv", fields = ('doc_id, 'word, 'count)))
  output(Tsv(args("output")))

  override def query = """select "word", "count" from "bow""""
}

/*
*  Now lets define a lingual source for the table that applies a function first
*
* To run you must use the lingual assembled jar
* ./scripts/scald.rb \
*   --local com.twitter.scaldual.tutorial.LingualTableLocalFunction \
*   --output /tmp/lingual_test.tsv
*/
case class LocalTableFunction()(implicit flowDef:FlowDef) extends FlatMappedLingualTable[(Int, String, Int), (String, Int)] {
  override def fieldNames = new Fields("cnv_word", "cnv_count")
  override def function = {case(index, word, count) => Some((word, count))}
  override def tmpDirectory = "/tmp"
  override def name = "bow"
  override def source = TypedTsv[(Int, String, Int)]("src/main/resources/tutorial/docBOW.tsv", f=new Fields("doc_id", "word", "count"))
}

class LingualTableLocalFunction(args:Args) extends LingualJob(args){
  table(LocalTableFunction())
  output(Tsv(args("output")))

  override def query = """select "cnv_count", "cnv_word" from "bow" """
}