import sbt._
import Keys._

/** based on https://github.com/harrah/xsbt/wiki/Getting-Started-Multi-Project */
object ApplicationBuild extends Build {

  // aggregate: running a task on the aggregate project will also run it on the aggregated projects.
  // dependsOn: a project depends on code in another project.
  lazy val root = Project(id = "big-data-pipeline",
    base = file(".")) aggregate(commons, producer, consumer, stream) dependsOn(producer, consumer, stream)//dependsOn(commons, producer)

  lazy val commons = Project(id = "commons",
    base = file("commons"))

  lazy val producer = Project(id = "producer",
    base = file("producer")) dependsOn(commons)

  lazy val consumer = Project(id = "consumer",
    base = file("consumer")) dependsOn(commons)

  lazy val stream = Project(id = "stream",
    base = file("stream")) dependsOn(commons)

  val jacksonVersion = "2.6.1"
  val log4jVersion = "2.4.1"
  val kafkaVersion = "0.8.2.2"

  override lazy val settings = super.settings ++
    Seq(scalaVersion := "2.11.7",
        scalacOptions := Seq("-feature", "-unchecked", "-deprecation", "-encoding", "utf8"),
        resolvers += "Cloudera repo" at "https://repository.cloudera.com/artifactory/cloudera-repos",
        resolvers += "Spring repo" at "http://repo.springsource.org/milestone",
        resolvers += "Clojar repo" at "http://clojars.org/repo",
        libraryDependencies ++= Seq(
          // JSON utilities
          "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
          "com.fasterxml.jackson.core"    % "jackson-core"         % jacksonVersion,
          "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonVersion,

          "commons-logging"          % "commons-logging" %  "1.2",
          "org.apache.logging.log4j" % "log4j-api"       % log4jVersion,
          "org.apache.logging.log4j" % "log4j-core"      % log4jVersion,
          // This routes requests to log4j 1.2 to log4j2. LEAVE IT BE
          "org.apache.logging.log4j" % "log4j-1.2-api"   % log4jVersion,
          "org.apache.kafka" %% "kafka" % kafkaVersion
            excludeAll(
            ExclusionRule(organization = "log4j", name = "log4j"),
            ExclusionRule(organization = "jline", name = "jline"),
            ExclusionRule(organization = "org.scala-lang.modules"))
        )
    )

}