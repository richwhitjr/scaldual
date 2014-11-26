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

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.source.TypedSequenceFile

trait LingualTable extends java.io.Serializable {
  import FileSourceExtensions._

  def source: FileSource
  def name: String
  def cleanup(m: Mode) {}
  def addToFlowDef(f: FlowDef, m: Mode) { f.addSource(name, source.sourceTap(m)) }
  def desc = {
    source match {
      case e:FileSource with DelimitedScheme =>
        "Class = %s\nTable = %s\nFields = %s".format(e.getClass.getCanonicalName, name, e.fields.toString)
      case _ => "Class = %s\n Name = %s".format(source.getClass.getCanonicalName, name)
    }
  }
}

trait LingualTableTyped[A] extends java.io.Serializable with LingualTable {
  override def source: FileSource with TypedSink[A] with Mappable[A]
}

abstract class FlatMappedLingualTable[A, B](implicit manifest: Manifest[B], tc: TupleConverter[B], ts: TupleSetter[B], @transient flowDef: FlowDef)
  extends java.io.Serializable with LingualTableTyped[A] {

  import Dsl._
  import FileSourceExtensions._
  import TDsl._

  def tmpFile: TypedSequenceFile[B] = {
    new TypedSequenceFile[B]("%s/%s.tmp".format(tmpDirectory, name)){
      override val fields = fieldNames
    }
  }
  def fieldNames: Fields
  def function: A => TraversableOnce[B]
  def tmpDirectory = "tmp"
  override def cleanup(m: Mode) { tmpFile.delete(m) }
  override def addToFlowDef(@transient f: FlowDef, m: Mode) {
    implicit val mode = m
    source.flatMap{ item: A => function(item) }.toPipe(fieldNames).write(tmpFile)
    f.addSource(name, tmpFile.sourceTap(mode))
  }
  override def desc = "Class = %s\nTable = %s\nFields = %s".format(source.getClass.getCanonicalName, name, fieldNames)
}

case class ScaldingLingualTable[A, B](override val name: String,
  override val source: FileSource with TypedSink[A] with Mappable[A],
  override val fieldNames: Fields,
  override val function: A => TraversableOnce[B])(implicit mf: Manifest[B], tc: TupleConverter[B], ts: TupleSetter[B], flowDef: FlowDef)
  extends FlatMappedLingualTable[A, B]