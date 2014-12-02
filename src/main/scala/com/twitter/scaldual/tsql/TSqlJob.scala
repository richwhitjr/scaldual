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
  val inputs  = args.list("input")

  if(inputs.size == 0)
    sys.error("Must give at least one file")

  inputs.zipWithIndex.foreach{case(input, index) =>
    val line = TypedPsv[String](input).toIterator

    if(!line.hasNext)
      sys.error("Given empty file")

    val firstLine = line.next()

    val columns = firstLine.split(delim).size
    val columnNames = 0.to(columns-1).map{v =>
      val col = Character.toUpperCase(Character.forDigit(v+10,36)).toString
      if(index == 0) col else col+index
    }

    val tableName = if(index == 0) "FILE" else s"FILE$index"

    table(tableName, Tsv(input, fields = new Fields(columnNames:_*)))
  }

  val outputFile = args("output")
  output(Tsv(outputFile))

  override def query = args("query")

  override def cascadeComplete(): Unit = {
    val headOpt = args.optional("head")
    if(headOpt.isDefined){
      println("Writing first %s lines:".format(headOpt.get))
      TypedPsv[String](outputFile)
        .toIterator
        .take(headOpt.get.toInt)
        .foreach(println)
    }
  }
}