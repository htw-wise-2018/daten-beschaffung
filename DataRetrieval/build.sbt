name := "HTW-ECCO"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/edu.ucar/netcdf
libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.0.0",
	"org.apache.spark" % "spark-sql_2.11" % "2.0.0",
	"edu.ucar" % "netcdf" % "4.2.20",
	"org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2",
	"org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.1"
)
