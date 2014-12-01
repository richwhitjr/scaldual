package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

import scala.collection.JavaConverters._

object ScadualBuild extends Build {
  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")
  
  val sharedSettings = Project.defaultSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ assemblySettings ++ Seq(
     organization := "com.twitter",
     scalaVersion := "2.10.4",
     javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
     javacOptions in doc := Seq("-source", "1.7"),
     libraryDependencies ++= Seq(
       "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
       "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
       "org.mockito" % "mockito-all" % "1.8.5" % "test"
     ),
     resolvers ++= Seq(
        "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
        "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "releases" at "https://oss.sonatype.org/content/repositories/releases",
        "Concurrent Maven Repo" at "http://conjars.org/repo",
        "Clojars Repository" at "http://clojars.org/repo",
        "Twitter Maven" at "http://maven.twttr.com",
        "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
      ),
      excludedJars in assembly <<= (fullClasspath in assembly) map {
        cp =>
          val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
            "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar")
          cp filter {
            jar => excludes(jar.data.getName)
          }
      },
      mergeStrategy in assembly <<= (mergeStrategy in assembly) {
        (old) => {
          case s if s.endsWith(".properties") => MergeStrategy.last
          case s if s.endsWith("defaultManifest.mf") => MergeStrategy.last
          case s if s.endsWith("version.txt") => MergeStrategy.last        
          case s if s.endsWith(".class") => MergeStrategy.last
          case s if s.endsWith("project.clj") => MergeStrategy.concat
          case s if s.endsWith(".html") => MergeStrategy.last
          case s if s.endsWith(".dtd") => MergeStrategy.last
          case s if s.endsWith(".xsd") => MergeStrategy.last
          case s if s.endsWith(".jnilib") => MergeStrategy.rename
          case s if s.endsWith("jansi.dll") => MergeStrategy.rename
          case x => old(x)
        }
      },
      javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m"),
      scalacOptions ++= Seq("-unchecked", "-deprecation"),
      logLevel in assembly := Level.Warn,
      libraryDependencies ++= Seq(
        "com.twitter" % "scalding-core_2.10" % "0.11.0" exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12"),
        "com.twitter" % "scalding-repl_2.10" % "0.11.0" exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12"),
        "com.twitter" % "scalding-commons_2.10" % "0.11.0" exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12"),
        "xerces" % "xercesImpl" % "2.9.1",
        "cascading" % "lingual-core" % "1.2.0" exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12"),
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "org.slf4j" % "log4j-over-slf4j" % "1.7.5" % "provided"
      )
  )
  
  lazy val scaldual = Project(
    id = "scaldual",
    base = file("."),
    settings = sharedSettings)
}
       