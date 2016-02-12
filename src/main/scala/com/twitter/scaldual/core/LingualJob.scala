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

import cascading.cascade.CascadeConnector
import cascading.flow.{ Flow, FlowDef }
import cascading.lingual.flow.SQLPlanner
import com.twitter.scalding._
import com.twitter.scalding.DateOps._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import java.util.TimeZone

trait HasDateRange {
  implicit def dateRange: DateRange
  implicit def tz: java.util.TimeZone
}

abstract class LingualDateJob(args:Args) extends LingualJob(args) with DefaultDateRangeJob with HasDateRange {
  override def defaultTimeZone = UTC
  override def cascadeBefore(){
    this.init
  }
}

abstract class LingualJob(args:Args) extends Job(args) {

  import FileSourceExtensions._

  private val tablesAdded = new mutable.HashSet[String]()

  protected val catalogs = new mutable.ListBuffer[LingualCatalog]()

  /**
   * Check if the user has set two outputs.  Lingual will pick one and not fail but the user
   *  may not expect this behavior
   */
  protected var outputSet = false

  /**
   * If the user created a scalding Pipe in the job the flowdef will have sources set.
   * By checking those we will know if we need to run Scalding or not.
   */
  private def hasScaldingFlow = flowDef.getSources.size() > 0

  /**
   * TODO: Add a cascades validation code here.
   * Should validate sources in both lingual and scalding
   */
  override def validate {}

  @transient
  protected val addedTables = new ListBuffer[LingualTable]

  /**
   * Called before the cascade starts
   */
  protected def cascadeBefore(): Unit = {}

  /**
   * Called once the cascade flows are complete
   */
  protected def cascadeComplete(): Unit = {}

  @transient
  override implicit val flowDef = {
    val name = getClass.getCanonicalName + "Scalding"
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  /**
   * Create a flow to contain the Lingual job.
   */
  @transient
  protected val lingualFlow = {
    val name = getClass.getCanonicalName + "Lingual"
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  override def config: Map[AnyRef, AnyRef] = super.config

  protected def query: String

  private def getFlow(flowToConnect: FlowDef): Try[Flow[_]] =
    Config.tryFrom(config).map { conf =>
      mode.newFlowConnector(conf).connect(flowToConnect)
    }

  protected def initializeCatalog(implicit dateRange:DateRange, tz:TimeZone){}

  protected def init(implicit dateRange:DateRange, tz:TimeZone) = {
    initializeCatalog(dateRange, tz)

    RegisteredTables(tablesAdded.toSet,
                     ParseQuery.tables(query),
                     catalogs.toList,
                     lingualFlow,
                     mode)(dateRange, flowDef)
  }

  override def run = {
    cascadeBefore()

    val planner = new SQLPlanner().setSql(query)

    lingualFlow.addAssemblyPlanner(planner)

    val flows = if (hasScaldingFlow) {
      Seq(getFlow(flowDef).get, getFlow(lingualFlow).get)
    } else {
      Seq(getFlow(lingualFlow).get)
    }

    flows.foreach{flow =>
      listeners.foreach(flow.addListener)
      stepListeners.foreach(flow.addStepListener)
      skipStrategy.foreach(flow.setFlowSkipStrategy)
      stepStrategy.foreach(flow.setFlowStepStrategy)
    }

    val cascade = new CascadeConnector().connect(flows: _*)
    cascade.complete()

    addedTables.foreach(_.cleanup(mode))
    addedTables.clear()

    cascadeComplete()

    val statsData = cascade.getCascadeStats
    handleStats(statsData)
    statsData.isSuccessful
  }

  def catalog(catalog:LingualCatalog) { catalogs += catalog}

  def table[A, B](table: FlatMappedLingualTable[A, B]) {
    tablesAdded += table.name
    addedTables += table
    table.addToFlowDef(lingualFlow, mode)
  }

  def table(name: String, f: FileSource) {
    tablesAdded += name
    lingualFlow.addSource(name, f.sourceTap(mode))
  }

  def output(f: FileSource) {
    if (outputSet) {
      sys.error("Can only have one output sink with a Lingual Job.")
    } else {
      outputSet = true
      lingualFlow.addSink("output", f.sinkTap(mode))
    }
  }
}