name := "producer"

version := "0.1"

val slf4jVersion = "1.7.10"
val kafkaVersion = "0.8.2.2"
val elasticsearchVersion = "1.7.3"
val stormVersion = "0.9.5"
val scalaCSVVersion = "1.2.2"

libraryDependencies ++= Seq(
  "org.elasticsearch" % "elasticsearch" % elasticsearchVersion,
  "org.apache.storm" % "storm-core" % stormVersion,
  "org.apache.storm" % "storm-kafka" % stormVersion,

  "com.github.tototoshi" %% "scala-csv" % scalaCSVVersion,
  "org.apache.commons" % "commons-math3" % "3.5"
)