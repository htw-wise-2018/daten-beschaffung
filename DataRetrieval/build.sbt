name := "HTW-ECCO"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.0.0",
	"org.apache.spark" % "spark-sql_2.11" % "2.0.0",
	"org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0",
	"org.scalatest" %% "scalatest" % "3.0.0",
	// Math Libraries
	// "org.jblas" % "jblas" % "1.2.3",
	// other dependencies here
	"org.scalanlp" %% "breeze" % "0.11.2",
	"org.json4s" %% "json4s-native" % "3.2.11",
	// native libraries greatly improve performance, but increase jar sizes.
	"org.scalanlp" %% "breeze-natives" % "0.11.2",
	// Nd4j scala api with netlib-blas backend
	// "org.nd4j" % "nd4s_2.10" % "0.5.0",
	"org.nd4j" % "nd4j-native-platform" % "0.5.0",
	"org.nd4j" %% "nd4j-kryo" % "0.5.0",
	"edu.ucar" % "opendap" % "4.6.0",
	"joda-time" % "joda-time" % "2.8.1",
	"org.joda" % "joda-convert" % "1.8.1",
	"com.joestelmach" % "natty" % "0.11",
	"edu.ucar" % "cdm" % "4.6.0",
	"org.datasyslab" % "sernetcdf" % "0.1.0",
	"org.mongodb" % "bson" % "3.9.0",
	"com.typesafe.akka" %% "akka-actor" % "2.5.18",
	"com.springml" % "spark-sftp_2.11" % "1.1.0"
)
