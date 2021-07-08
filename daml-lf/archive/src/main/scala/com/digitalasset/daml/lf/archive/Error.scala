// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

class Error(val msg: String) extends RuntimeException(msg)

object Error {

  import DarReader.ZipEntries

  final case class InvalidDar(entries: ZipEntries, cause: Throwable)
      extends Error(s"Invalid DAR: ${darInfo(entries): String}")

  final case class InvalidZipEntry(name: String, entries: ZipEntries)
      extends RuntimeException(
        s"Invalid zip entryName: ${name: String}, DAR: ${darInfo(entries): String}"
      )

  final case class InvalidLegacyDar(entries: ZipEntries)
      extends Error(s"Invalid Legacy DAR: ${darInfo(entries)}")

  final case class ZipBomb()
      extends Error(s"An entry is too large, rejected as a possible zip bomb")

  private def darInfo(entries: ZipEntries): String =
    s"${entries.name}, content: [${darFileNames(entries).mkString(", "): String}}]"

  private def darFileNames(entries: ZipEntries): Iterable[String] =
    entries.entries.keys

  final case class Parsing(override val msg: String) extends Error(msg)
}
