val spark = "org.apache.spark" % "spark-core_2.11" % "1.5.1"
val sparksql= "org.apache.spark" % "spark-sql_2.11" % "1.5.1"
val scalatest =  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"


lazy val commonSettings = Seq(
  organization := "com.peter",
  version := "0.1.0",
  scalaVersion := "2.11.4"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "sampleProject",
    libraryDependencies += spark,
    libraryDependencies += sparksql,
    libraryDependencies += scalatest
  )
