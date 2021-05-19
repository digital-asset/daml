// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats.util

import java.nio.file.Path

import scalaz.\/

import scala.io.{BufferedSource, Source}

object ReadFileSyntax {
  implicit class PathSourceOps(val path: Path) extends AnyVal {
    def contentsAsString: Throwable \/ String =
      withSource(_.mkString)

    def withSource(f: BufferedSource => String): Throwable \/ String =
      \/.fromTryCatchNonFatal {
        val source = Source.fromFile(path.toFile)
        try f(source)
        finally source.close()
      }
  }
}
