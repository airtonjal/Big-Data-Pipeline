name := "producer"

version := "0.1"

val slf4jVersion = "1.7.10"

libraryDependencies ++= Seq(

  "org.slf4j" % "slf4j-api"    % slf4jVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion
)