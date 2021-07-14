// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.File

import scala.util.{Failure, Success, Try}

/** Can parse DARs and DALFs.
  */
final class GenUniversalArchiveReader[A](
    reader: GenReader[A]
) {

  /** Reads a DAR from a File. */
  def readFile(
      file: File,
      entrySizeThreshold: Int = GenDarReader.EntrySizeThreshold,
  ): Try[Dar[A]] =
    SupportedFileType.supportedFileType(file).flatMap {
      case SupportedFileType.DarFile =>
        GenDarReader(reader).readArchiveFromFile(file, entrySizeThreshold)
      case SupportedFileType.DalfFile =>
        Try(Dar(reader.fromFile(file), List.empty))
    }

}

object SupportedFileType {
  def supportedFileType(f: File): Try[SupportedFileType] =
    if (DarFile.matchesFileExtension(f)) Success(DarFile)
    else if (DalfFile.matchesFileExtension(f)) Success(DalfFile)
    else Failure(UnsupportedFileExtension(f))

  sealed abstract class SupportedFileType(fileExtension: String) extends Serializable with Product {
    def matchesFileExtension(f: File): Boolean = f.getName.endsWith(fileExtension)
  }
  final case object DarFile extends SupportedFileType(".dar")
  final case object DalfFile extends SupportedFileType(".dalf")

  case class UnsupportedFileExtension(file: File)
      extends RuntimeException(s"Unsupported file extension: ${file.getAbsolutePath}")
}
