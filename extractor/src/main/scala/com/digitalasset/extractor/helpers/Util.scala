// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.helpers

import java.net.URI
import java.nio.file.{Files, Path, Paths}

object Util {

  @annotation.varargs
  def guessRelativeFileLocation(filenames: String*): URI = {
    val uri = guessPath(filenames)
    Paths.get(".").toAbsolutePath.relativize(uri).toUri
  }

  @annotation.varargs
  def guessFileLocation(filenames: String*): URI = guessPath(filenames).toUri

  private def cwd = Paths.get(".").toAbsolutePath

  def guessPath(filenames: Seq[String]): Path = {
    def folders(from: Path): Stream[Path] =
      if (from == null) Stream.empty else from #:: folders(from.getParent)

    def guess(from: Path): Stream[Path] =
      folders(from).flatMap { d =>
        filenames.toStream.map(d.resolve)
      }

    val guesses = guess(cwd)

    guesses
      .find(Files.exists(_))
      .getOrElse(throw new IllegalStateException(s"""Could not find ${filenames
                                                      .mkString(", ")}, having searched:
                                         |${guesses.mkString("\n")}""".stripMargin))
  }

}
