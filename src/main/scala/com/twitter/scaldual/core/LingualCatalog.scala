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

/**
 * Base trait for a Lingual Catalog.  This is best thought of as a collection of tables.
 * Lingual/Scalding has the ability to automatically add a table to the flow but requires a mapping from
 * name to source.  Subclasses of this trait provide this mapping.
 *
 * User: rwhitcomb
 * Date: 8/18/14
 * Time: 8:24 AM
 */

trait LingualCatalog {
  def tables:Map[String, LingualTable]

  override def toString = {
    tables.map{case(name, table) => "name=%s,table=%s".format(name, table.toString)}.mkString("\n")
  }
}