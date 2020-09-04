// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats.util

import java.io.File

import scalaz.\/

import scala.io.{BufferedSource, Source}

object ReadFileSyntax {
  implicit class FileSourceOps(val path: File) extends AnyVal {
    def contentsAsString: Throwable \/ String =
      withSource(_.mkString)

    def withSource(f: BufferedSource => String): Throwable \/ String =
      \/.fromTryCatchNonFatal {
        val source = Source.fromFile(path)
        try f(source)
        finally source.close()
      }
  }
}
