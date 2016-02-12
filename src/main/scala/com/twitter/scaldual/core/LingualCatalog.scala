package com.twitter.scaldual.core

/**
 * Base trait for a Lingual Catalog.  This is best thought of as a collection of tables.
 * Lingual/Scalding has the ability to automatically add a table to the flow but requires a mapping from
 * name to source.  Subclasses of this trait provide this mapping.
 */

trait LingualCatalog {
  def tables:Map[String, LingualTable]

  override def toString = {
    tables.map{case(name, table) => "name=%s,table=%s".format(name, table.toString)}.mkString("\n")
  }
}