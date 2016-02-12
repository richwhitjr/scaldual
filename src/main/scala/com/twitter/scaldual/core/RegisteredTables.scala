package com.twitter.scaldual.core

import cascading.flow.FlowDef
import com.twitter.scalding.{Mode, DateRange}

/**
 * Automatically added tables to flowdef based on a parsed query.
 * The list of catalogs is where the lookup is done based on table name.
 */

object RegisteredTables {
  def apply(registered:Set[String],
            tableNames:Set[String],
            catalogs:List[LingualCatalog],
            fd:FlowDef,
            m:Mode)(implicit dateRange:DateRange, scaldingFlowDef:FlowDef){

    if(catalogs.nonEmpty){
      val tableMap = catalogs.map(_.tables).reduce(_ ++ _)
      (tableNames &~ registered).flatMap{tableName => tableMap.get(tableName).map(_.addToFlowDef(fd, m))}
    }
  }
}