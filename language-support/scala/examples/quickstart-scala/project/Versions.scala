// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {

  private val daSdkVersionKey = "da.sdk.version"

  private val errorMsg =
    s"Error: cannot determine DA SDK version, either specify it with '-D${daSdkVersionKey}=<VERSION>' or use 'daml.yaml' with configured 'sdk-version' field."

  val daSdkVersion: String = sys.props
    .get(daSdkVersionKey)
    .getOrElse(
      sdkVersionFromFile(new java.io.File("daml.yaml")).fold(
        error => { println(errorMsg); throw error },
        identity
      )
    )

  println(s"$daSdkVersionKey = ${daSdkVersion: String}")

  private def sdkVersionFromFile(file: java.io.File): Either[io.circe.Error, String] = {
    import io.circe.yaml.parser
    import io.circe.ParsingFailure
    import scala.util.Try
    for {
      str <- Try(sbt.IO.read(file)).toEither.left.map(e =>
        ParsingFailure(s"Cannot read file: $file", e))
      yaml <- parser.parse(str)
      version <- yaml.hcursor.downField("sdk-version").as[String]
    } yield version
  }
}
