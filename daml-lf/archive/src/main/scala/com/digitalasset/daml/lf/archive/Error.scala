// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File

sealed abstract class Error(val msg: String) extends RuntimeException(msg)

object Error {

  final case class Internal(where: String, message: String) extends Error(s"IO error: $message")

  final case class IO(where: String, cause: java.io.IOException) extends Error(s"IO error: $cause")

  import GenDarReader.ZipEntries

  final case class InvalidDar(entries: ZipEntries, cause: Throwable)
      extends Error(s"Invalid DAR: ${darInfo(entries): String}")

  final case class InvalidZipEntry(name: String, entries: ZipEntries)
      extends Error(s"Invalid zip entryName: ${name: String}, DAR: ${darInfo(entries): String}")

  final case class InvalidLegacyDar(entries: ZipEntries)
      extends Error(s"Invalid Legacy DAR: ${darInfo(entries)}")

  final case object ZipBomb extends Error(s"An entry is too large, rejected as a possible zip bomb")

  private def darInfo(entries: ZipEntries): String =
    s"${entries.name}, content: [${darFileNames(entries).mkString(", "): String}}]"

  private def darFileNames(entries: ZipEntries): Iterable[String] =
    entries.entries.keys

  final case class DarManifestReaderException(message: String) extends Error(message)

  final case class UnsupportedFileExtension(file: File)
      extends Error(s"Unsupported file extension: ${file.getAbsolutePath}")

  final case class Parsing(override val msg: String) extends Error(msg)
}
