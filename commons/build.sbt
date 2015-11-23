name := "commons"

version := "0.1"

val slf4jVersion = "1.7.10"
val scalaCSVVersion = "1.2.2"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.apache.commons" % "commons-io" % "1.3.2",
  "com.github.tototoshi" %% "scala-csv" % scalaCSVVersion
)

