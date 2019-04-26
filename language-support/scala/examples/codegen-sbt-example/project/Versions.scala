// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {
  private val daSdkVersionKey = "da.sdk.version"

  val daSdkVersion: String = sys.props.get(daSdkVersionKey).getOrElse("100.12.12")
  println(s"$daSdkVersionKey: $daSdkVersion")

  lazy val detectedOs: String = sys.props("os.name") match {
    case "Mac OS X" => "osx"
    case _ => "linux"
  }
}
