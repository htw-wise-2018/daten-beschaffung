name := "HTW-ECCO"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.0.0",
	"org.apache.spark" % "spark-sql_2.11" % "2.0.0"
)
