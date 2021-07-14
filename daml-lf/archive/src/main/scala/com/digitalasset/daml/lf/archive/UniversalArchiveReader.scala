// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.nameof.NameOf

import java.io.File

/** Can parse DARs and DALFs.
  */
final class GenUniversalArchiveReader[A](
    reader: GenReader[A]
) {

  /** Reads a DAR from a File. */
  def readFile(
      file: File,
      entrySizeThreshold: Int = GenDarReader.EntrySizeThreshold,
  ): Result[Dar[A]] =
    SupportedFileType.supportedFileType(file).flatMap {
      case SupportedFileType.DarFile =>
        GenDarReader(reader).readArchiveFromFile(file, entrySizeThreshold)
      case SupportedFileType.DalfFile =>
        attempt(NameOf.qualifiedNameOfCurrentFunc, Dar(reader.fromFile(file), List.empty))
    }

  @throws[Error]
  def assertReadFile(file: File): Dar[A] =
    assertRight(readFile(file))

}

object SupportedFileType {
  def supportedFileType(f: File): Result[SupportedFileType] =
    if (DarFile.matchesFileExtension(f)) Right(DarFile)
    else if (DalfFile.matchesFileExtension(f)) Right(DalfFile)
    else Left(Error.UnsupportedFileExtension(f))

  sealed abstract class SupportedFileType(fileExtension: String) extends Serializable with Product {
    def matchesFileExtension(f: File): Boolean = f.getName.endsWith(fileExtension)
  }
  final case object DarFile extends SupportedFileType(".dar")
  final case object DalfFile extends SupportedFileType(".dalf")

}
