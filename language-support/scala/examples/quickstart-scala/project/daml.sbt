import Versions._
import Artifactory._

resolvers ++= daResolvers

libraryDependencies ++= Seq(
  "com.daml.scala" %% "codegen-main" % daSdkVersion,
)
