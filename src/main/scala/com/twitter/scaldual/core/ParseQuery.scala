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

package com.twitter.scaldual.core

import java.util
import org.eigenbase.sql.SqlKind._
import org.eigenbase.sql._
import org.eigenbase.sql.parser.SqlParser
import scala.collection.JavaConversions._

/**
 * Created by IntelliJ IDEA.
 * User: rwhitcomb
 * Date: 8/10/14
 * Time: 8:23 PM
 *
 */

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