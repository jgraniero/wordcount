import AssemblyKeys._

name := "wordcount"

version := "0.0.1"

organization := "com.nrelate"

resolvers ++= Seq(
  "clojars" at "http://clojars.org/repo"
)

libraryDependencies ++= Seq(
  "storm" % "storm" % "0.9.0.1" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.27"
)
