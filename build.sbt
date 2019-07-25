

lazy val commonSettings= Seq(

  name := "kafka-twitter-data-integration",
  version := "0.1",
  scalaVersion := "2.12.8",

  // Testing
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % "test",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.11",
  libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % Test,
  libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0",
  libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0",
  libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5",
  libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.9",

  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"),
  test := {},
  mainClass := Some("Main"),

  scalacOptions += "-deprecation",
  scalacOptions += "-feature",
  scalacOptions ++= Seq("-Xmax-classfile-name","128"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
  // Use dependency injected routes in Play modules
  //routesGenerator := InjectedRoutesGenerator
)


lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
