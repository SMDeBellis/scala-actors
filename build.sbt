
ThisBuild / scalaVersion := "3.3.1"

ThisBuild / organization := "com.example"

ThisBuild / javacOptions ++= Seq(
  "--release",
  "21"
)

ThisBuild / javaOptions ++= Seq(
  "--add-opens",
  "java.base/java.lang=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "scala-actors",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"
    ),
    Test / parallelExecution := false,
    Compile / compileOrder := CompileOrder.JavaThenScala
  )
