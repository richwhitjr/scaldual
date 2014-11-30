package com.twitter.scaldual.tsql

import cascading.tuple.Fields
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
class TSqlJob(args:Args) extends LingualJob(args){
  val delim = args.optional("delim").getOrElse("\t")
  val input  = args("input")

  val line = TypedPsv[String](input).toIterator

  if(!line.hasNext)
    sys.error("Given empty file")

  val firstLine = line.next()
    
  val columns = firstLine.split(delim).size  
  val columnNames = 0.to(columns-1).map(v => Character.toUpperCase(Character.forDigit(v+10,36)).toString)
  
  table("FILE", Tsv(input, fields = new Fields(columnNames:_*)))
  output(Tsv(args("output")))
  override def query = args("query")
}