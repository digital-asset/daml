import Versions._
import Artifactory._

resolvers ++= daResolvers

libraryDependencies ++= Seq(
  "com.digitalasset" % "damlc" % daSdkVersion classifier detectedOs,
  "com.daml.scala" %% "codegen-main" % daSdkVersion,
)
