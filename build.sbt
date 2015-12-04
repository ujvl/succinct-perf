name := "succinct-perf"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "amplab" % "succinct" % "0.1.6"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"

libraryDependencies += "commons-cli" % "commons-cli" % "1.3.1"
