// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {

  private val daSdkVersionKey = "da.sdk.version"

  val daSdkVersion: String = sys.props.get(daSdkVersionKey).getOrElse(sdkVersionFromFile())
  println(s"$daSdkVersionKey = ${daSdkVersion: String}")

  private def sdkVersionFromFile(): String =
    sbt.IO.read(new sbt.File("./SDK_VERSION").getAbsoluteFile).trim
}
