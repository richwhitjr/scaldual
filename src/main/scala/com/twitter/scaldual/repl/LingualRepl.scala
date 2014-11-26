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

import cascading.flow.{FlowDef, Flow}
import com.twitter.scaldual.core.{LingualCatalog, LingualJob, LingualTable}
import com.twitter.scalding._
import java.util.TimeZone
import scala.collection.mutable

/**
 * Object containing various implicit conversions required to create Lingual flows in the REPL.
 */
object LingualRepl {

  import FileSourceExtensions._

  implicit val tz = TimeZone.getTimeZone("UTC")
  implicit def dateParser: DateParser = DateParser.default

  var lastFlow:Option[Flow[_]] = None
  var outputTap:Option[FileSource] = None

  private val tablesAdded = new mutable.HashSet[String]()

  private val replAddedTables = new mutable.ListBuffer[LingualTable]

  private val replCatalogs = new mutable.ListBuffer[LingualCatalog]()

  var lingualFlowDef: FlowDef = getEmptyFlowDef("ScaldingLingualShell")

  def resetFlowDefLingual(){
    ReplImplicits.resetFlowDef()
    lingualFlowDef = getEmptyFlowDef("ScaldingLingualShell")
  }

  def lingualConfig = {
    val conf = Config.default

    val replCodeJar = ScaldingShell.createReplCodeJar()
    val tmpJarsConfig: Map[String, String] =
      replCodeJar match {
        case Some(jar) =>
          Map("tmpjars" -> { conf.get("tmpjars").map(_ + ",").getOrElse("").concat("file://" + jar.getAbsolutePath)})
        case None =>
          Map()
      }

    conf ++ tmpJarsConfig ++ Map("mapreduce.job.sharedcache.mode" -> "disabled")
  }

  /**
   * Gets a new, empty, flow definition.
   * @return a new, empty flow definition.
   */
  def getEmptyFlowDef(name:String): FlowDef = {
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  def lingual(lingualQuery:String)(implicit fd: FlowDef, md: Mode, dateRange:DateRange) {
    println("Running Query %s".format(lingualQuery))
    def getJob(args: Args, inmode: Mode, inputFlowDef:FlowDef): LingualJob = new LingualJob(args) {
      @transient
      override implicit val flowDef = inputFlowDef
      override val addedTables = replAddedTables
      override implicit def mode = inmode
      override def query = lingualQuery
      override val lingualFlow = lingualFlowDef
      override val catalogs = replCatalogs
      override def cascadeBefore() {
        println("Setting up job...")
        this.init

        //If no output is defined add one
        if(outputTap.isEmpty){
          val defaultOutput = "lingual/table_output.tsv"
          println("Default output to %s".format(defaultOutput))
          output(Tsv(defaultOutput))
        }
      }
      override def cascadeComplete() {
        println("Cascades complete, cleaning up")
        resetFlowDefLingual()
        tablesAdded.clear()
      }
      override def config: Map[AnyRef, AnyRef] = super.config ++ lingualConfig.toMap.toMap
    }

    getJob(new Args(Map()), md, fd).run
  }

  def desc(f:FileSource with DelimitedScheme){
    println("Table=%s, Fields=%s".format(f.getClass.getCanonicalName, f.fields.toString))
  }

  def desc(lt:LingualTable){
    println(lt.desc)
  }

  def table[A, B](table:LingualTable)(implicit md: Mode){
    replAddedTables += table
    tablesAdded += table.name
    table.addToFlowDef(lingualFlowDef, md)
  }

  def catalog(catalog:LingualCatalog) { replCatalogs += catalog}

  def table(name:String, f:FileSource)(implicit md: Mode) {
    tablesAdded += name
    lingualFlowDef.addSource(name, f.sourceTap(md))
  }

  def output(f:FileSource)(implicit md: Mode) {
    val ot = f.sinkTap(md)
    outputTap = Some(f)
    lingualFlowDef.addSink("output", ot)
  }
}