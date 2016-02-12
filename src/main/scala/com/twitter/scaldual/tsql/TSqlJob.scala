package com.twitter.scaldual.tsql

import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scaldual.core._

/*
* This job is meant to help run SQL queries on top of data files.
*
* It will take the first line of each file given and calculcate the number of columns.
* Each column will be assigned a letter of the alphabet. For more then one text file
* a index number will be appended to the end of the table name and columns.
*
* For example if you run the job like:
*
* --input some_file.tsv some_other_file.tsv
*
* some_file.tsv table will be FILE
* some_other_file.tsv table will be named FILE1
*
* The columns of FILE will be A-Z while FILE1 will be A1-Z1
*
* You can then write queries like "select A, A1 from FILE join FILE1 on FILE.B = FILE1.B1"
*
*/
class TSqlJob(args:Args) extends LingualJob(args){
  val delim = args.optional("delim").getOrElse("\t")
  val inputs  = args.list("input")

  if(inputs.isEmpty)
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