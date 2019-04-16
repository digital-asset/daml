// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {
  val sdkVersion: String = sdkVersionFromSysProps().getOrElse("100.12.6")

  println(s"DA sdkVersion: $sdkVersion")

  lazy val detectedOs: String = sys.props("os.name") match {
    case "Mac OS X" => "osx"
    case _ => "linux"
  }

  private def sdkVersionFromSysProps(): Option[String] =
    sys.props.get("DA.sdkVersion").map(_.toString)
}
