name := "spark-playground"

version := "0.1"

scalaVersion := "2.11.11"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.jcenterRepo,
  Resolver.mavenLocal,
  "Seeking Alpha" at "https://maven.pkg.github.com/seekingalpha/*"
)

// @formatter:off
libraryDependencies ++= {
  val sparkVersion = "2.4.8"
  Seq(
    "org.slf4j"                      % "slf4j-api"                   % "1.7.30",
    "ch.qos.logback"                 % "logback-classic"             % "1.2.3",
    "org.apache.spark"              %% "spark-sql"                   % sparkVersion,
    "org.apache.spark"              %% "spark-streaming"             % sparkVersion,
    "org.apache.spark"              %% "spark-mllib"                 % sparkVersion,
    "org.apache.spark"              %% "spark-streaming-kinesis-asl" % sparkVersion,
    "io.delta"                      %% "delta-core"                  % "0.6.1",
    "nl.basjes.parse.useragent"      % "yauaa"                       % "5.23",
    "com.databricks"                %% "spark-csv"                   % "1.5.0",
    "com.twitter"                   %% "util-collection"             % "19.1.0",
    "org.joda"                       % "joda-convert"                % "2.2.1",
    "com.sanoma.cda"                %% "maxmind-geoip2-scala"        % "1.5.5",
    "com.opencsv"                    % "opencsv"                     % "5.1",
    // "com.linkedin"                  %% "isolation-forest"            % "2.0.4",
    // "com.seekingalpha"               % "data-contract"               % "1.14.0",
    "com.github.scopt"              %% "scopt"                       % "4.0.1",
    "com.amazon.deequ"               % "deequ"                       % "1.0.5",
    "mysql"                          % "mysql-connector-java"        % "8.0.25",
    "com.sanoma.cda"                %% "maxmind-geoip2-scala"        % "1.5.5",

  // test resources
    "org.specs2"                    %% "specs2-core"                 % "4.6.0" % Test
  )
}
// @formatter:on

// in case you have a higher version of jackson-databind in your code, add the following:
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

/*
 * dont forget to add the following line to project/assembly.sbt:
 * addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
 */
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "log4j.propreties" => MergeStrategy.first
  // ----
  // required for spark-sql to read different data types (e.g. parquet/orc/csv...)
  // ----
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.first
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

mainClass in assembly := Some("me.rotemfo.SparkApp")

test in assembly := {}