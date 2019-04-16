// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

object Versions {
  lazy val sdkVersion: String = "100.12.6"

  lazy val detectedOs: String = sys.props("os.name") match {
    case "Mac OS X" => "osx"
    case _ => "linux"
  }
}
