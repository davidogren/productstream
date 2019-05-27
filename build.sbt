name := "productstream"

version := "1.0"

scalaVersion := "2.12.7"

lazy val akkaVersion = "2.5.23"
lazy val alpakkaVersion = "1.0.1"
lazy val logbackVersion = "1.2.3"
lazy val jacksonVersion = "2.9.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % alpakkaVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion,
  "junit" % "junit" % "4.12")
