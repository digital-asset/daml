// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {

  private val daSdkVersionKey = "da.sdk.version"

  val daSdkVersion: String = sys.props.get(daSdkVersionKey).getOrElse(sdkVersionFromFile())
  println(s"$daSdkVersionKey = ${daSdkVersion: String}")

  private val darFileKey = "dar.file"

  val darFile = sys.props
    .get(darFileKey)
    .map(s => new sbt.File(s))
    .getOrElse(new sbt.File(s"./dist/${projectNameFromConfig(): String}.dar"))
  println(s"$darFileKey = ${darFile.getAbsolutePath: String}")

  lazy val detectedOs: String = sys.props("os.name") match {
    case "Mac OS X" => "osx"
    case _ => "linux"
  }

  private def sdkVersionFromFile(): String =
    "10" + sbt.IO.read(new sbt.File("./SDK_VERSION").getAbsoluteFile).trim

  private def projectNameFromConfig(): String =
    sbt.IO
      .readLines(new sbt.File("./daml.yaml").getAbsoluteFile)
      .find(_.startsWith("name:"))
      .map(_.replaceFirst("name:", "").trim)
      .getOrElse(throw new IllegalStateException(s"Cannot read project name from a config file"))
}
