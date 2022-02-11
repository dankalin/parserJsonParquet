name := "parserJsonParquet"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}