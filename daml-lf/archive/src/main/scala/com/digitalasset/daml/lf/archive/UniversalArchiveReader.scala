// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.{File, FileInputStream, InputStream}
import java.util.zip.ZipInputStream

import scala.util.{Failure, Success, Try, Using}

/** Can parse DARs and DALFs.
  */
case class UniversalArchiveReader(
    entrySizeThreshold: Int = GenDarReader.EntrySizeThreshold
) {
  import SupportedFileType._

  /** Reads a DAR from a File. */
  def readFile(file: File): Try[Dar[ArchivePayload]] =
    supportedFileType(file).flatMap {
      case DarFile =>
        Using(new ZipInputStream(new FileInputStream(file)))(readDarStream(file.getName, _)).flatten
      case DalfFile => Using(new FileInputStream(file))(readDalfStream)
    }

  /** Reads a DAR from an InputStream. This method takes care of closing the stream! */
  def readDarStream(fileName: String, dar: ZipInputStream): Try[Dar[ArchivePayload]] =
    DarReader.readArchive(fileName, dar, entrySizeThreshold)

  /** Reads a DALF from an InputStream. This method takes care of closing the stream! */
  def readDalfStream(dalf: InputStream): Dar[ArchivePayload] =
    Dar(ArchiveReader.fromInputStream(dalf), List.empty)

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
