// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats.util

import java.nio.file.Path

import scalaz.\/

import scala.io.{BufferedSource, Source}

private[stats] object ReadFileSyntax {
  implicit class PathSourceOps(val path: Path) extends AnyVal {
    def contentsAsString: String \/ String =
      withSource(_.mkString)

    private def withSource(f: BufferedSource => String): String \/ String =
      \/.attempt {
        val source = Source.fromFile(path.toFile)
        try f(source)
        finally source.close()
      }(_.getMessage)
  }
}
