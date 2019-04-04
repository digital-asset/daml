import Versions._
import Artifactory._

resolvers ++= daResolvers

libraryDependencies ++= Seq(
  "com.digitalasset" % "damlc" % sdkVersion classifier detectedOs,
  "com.daml.scala" %% "codegen-main" % sdkVersion,
)
