sparkVersion := "2.2.0"
resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)
sparkComponents ++= Seq("sql")
name := "NYTrafficAnalyzer"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies += "com.koddi" %% "geocoder" % "1.1.0" from "file:///home/shivesh/.ivy2/cache/com.koddi/geocoder_2.11/srcs/geocoder_2.11-1.1.0-sources.jar"
libraryDependencies+=  "com.databricks" %% "spark-csv" % "1.5.0"