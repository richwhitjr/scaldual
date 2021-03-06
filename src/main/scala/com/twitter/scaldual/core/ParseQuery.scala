package com.twitter.scaldual.core

import java.util
import org.eigenbase.sql.SqlKind._
import org.eigenbase.sql._
import org.eigenbase.sql.parser.SqlParser
import scala.collection.JavaConversions._

object ParseQuery {
  private[ParseQuery] val enumerablePattern = "(order by .*)|(limit .*)"

  def flatten(node:SqlNode, list:util.ArrayList[SqlNode]):Unit ={
    node.getKind match {
      case JOIN =>
        val join = node.asInstanceOf[SqlJoin]
        flatten(join.getLeft, list)
        flatten(join.getRight,list)
      case AS =>
        val call = node.asInstanceOf[SqlCall]
        flatten(call.operands(0), list)
      case _ =>
        list.add(node)
        ()
    }
  }

  def flatten(node:SqlNode):util.ArrayList[SqlNode] = {
    val list = new util.ArrayList[SqlNode]()
    flatten(node, list)
    list
  }

  def parse(query:String):SqlSelect = {
    //Odd issues with Optiq not parsing order by, limit as select type.
    //Remove enumerable types for now until newer version of Optiq is used by lingual.
    val cleanedQuery = query.replaceAll(enumerablePattern, "")

    val node = new SqlParser(cleanedQuery).parseStmt()
    if(node.getKind.equals(SELECT)){
      node.asInstanceOf[SqlSelect]
    } else {
      throw new UnsupportedOperationException("Parse is only available for select currently")
    }
  }

  def tables(query:String):Set[String] = {
    try {
      flatten(parse(query).getFrom).map(_.toString).toSet
    } catch {
      case e:UnsupportedOperationException => Set()
    }
  }
}