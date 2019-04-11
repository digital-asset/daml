
name := "daml-lf"
scalaVersion := "2.12.6"

//TODO(nic): These options came from the build.sbt which was deleted back in March
// check they are all still required

lazy val commonSettings = Seq(
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-parameters"),
  scalacOptions ++= Seq(
    "-feature", // doesn't allow advance features of the language without explict import (higherkinds, implicits)
    "-target:jvm-1.8",
    "-encoding",
    "UTF-8",
    "-unchecked", // more detailed information about type-erasure related warnings
    "-deprecation", // warn if using deprecated stuff
    "-Xfuture",
    "-Xlint:_,-unused",
    "-Xmacro-settings:materialize-derivations", // better error reporting for pureconfig
    "-Xfatal-warnings",
    "-Yno-adapted-args", // adapted args is a deprecated feature: `def foo(a: (A, B))` can be called with `foo(a, b)`. properly it should be `foo((a,b))`
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen", // Warn about implicit conversion between numerical types
    "-Ywarn-value-discard", // Gives a warning for functions declared as returning Unit, but the body returns a value
    "-Ywarn-unused-import",
    "-Ywarn-unused",
  ),
  scalacOptions in (Compile, console) ~= (_.filterNot(_ == "-Xfatal-warnings")),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
)

// Versions are derived from ../dependencies.yaml // TODO(nic): this should be automated!

lazy val protobuf = "com.google.protobuf" % "protobuf-java" % "3.6.1"
lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
lazy val scalaz = "org.scalaz" %% "scalaz-core" % "7.2.24"
lazy val scalaz_scalacheck_binding = "org.scalaz" %% "scalaz-scalacheck-binding" % "7.2.24-scalacheck-1.14"
lazy val scalapb_runtime = "com.thesamet.scalapb" %% "scalapb-runtime" % "0.8.4"
lazy val parser_combinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
lazy val paiges = "org.typelevel" %% "paiges-core" % "0.2.1"
lazy val scalameter_core = "com.storm-enroute" %% "scalameter-core" % "0.10.1" % "test"


lazy val data_deps = Seq(scalatest, scalacheck, scalaz , scalaz_scalacheck_binding)
lazy val transaction_deps = Seq(scalaz, protobuf, scalatest, scalacheck, scalapb_runtime)
lazy val lfpackage_deps = scalatest +: data_deps
lazy val interp_deps = Seq(paiges, scalatest, scalacheck, scalapb_runtime)
lazy val validation_deps = Seq(scalatest, scalacheck)
lazy val engine_deps = Seq(protobuf, scalatest, scalacheck, scalameter_core)
lazy val testing_deps = engine_deps

lazy val repl_deps = Seq(
  // featurful line reader for the repl
  "org.jline" % "jline" % "3.7.1",
  "org.jline" % "jline-reader" % "3.7.1",
  // parser combinators for parsing values in the repl
  parser_combinators,
  // colourful pretty printing of arbitrary values
  "com.lihaoyi" %% "pprint" % "0.5.3",
  // scala implementation of wadler's pretty printer
  paiges
)


//TODO(nic): DarReaderTest fails

lazy val archive = (project in file("archive"))
  .dependsOn(data)
  .settings(
    libraryDependencies ++= Seq(protobuf, scalatest)
  )

lazy val data = (project in file("data"))
  .settings(
	libraryDependencies ++= data_deps
  )

lazy val engine = (project in file("engine"))
  .dependsOn(data, transaction, lfpackage, interpreter, validation, archive)
  .settings(
    libraryDependencies ++= engine_deps
  )

lazy val interface = (project in file("interface"))
  .dependsOn(archive, parser % "test->test")
  .settings(	 
    libraryDependencies ++= Seq(scalaz, scalatest)
  )
	
lazy val interpreter = (project in file("interpreter"))
  .dependsOn(data, transaction, lfpackage, archive, validation, parser % "test->test")
  .settings(
    libraryDependencies ++= interp_deps
  )

lazy val lfpackage = (project in file("lfpackage"))
  .dependsOn(data, archive)
  .settings(
	libraryDependencies ++= lfpackage_deps
  )

lazy val parser = (project in file("parser"))
  .dependsOn(lfpackage)
  .settings(
    libraryDependencies ++= Seq(parser_combinators, scalatest, scalacheck)
  )

lazy val repl = (project in file("repl"))
  .dependsOn(interpreter, `scenario-interpreter`, validation)
  .settings(
     libraryDependencies ++= repl_deps
  )

lazy val `scenario-interpreter` = (project in file("scenario-interpreter"))
  .dependsOn(interpreter)
  .settings(
    libraryDependencies ++= interp_deps
  )

lazy val testingTools = (project in file("testing-tools"))
  .dependsOn(archive, interpreter, `scenario-interpreter`, engine)
  .settings(
    libraryDependencies ++= testing_deps
  )	

// TODO(nic): transaction/test fails to compile -- needs ValueGenerators
// which is in transactionScalacheck - but that causes a recursive dep!

lazy val transaction = (project in file("transaction"))
  .dependsOn(data, archive) //, transactionScalacheck)
  .settings(
    libraryDependencies ++= transaction_deps
  )

lazy val transactionScalacheck = (project in file("transaction-scalacheck"))
  .dependsOn(data, archive, transaction)
  .settings(
    libraryDependencies ++= transaction_deps
  )

lazy val validation = (project in file("validation"))
  .dependsOn(lfpackage, parser % "test->test")
  .settings(
    libraryDependencies ++= validation_deps
  )
