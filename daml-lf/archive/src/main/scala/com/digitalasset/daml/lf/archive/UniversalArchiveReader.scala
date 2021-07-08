// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.{File, FileInputStream, InputStream}
import java.util.zip.ZipInputStream

import scala.util.{Failure, Success, Try}
import com.daml.lf.data.TryOps.Bracket.bracket

/** Can parse DARs and DALFs.
  */
case class UniversalArchiveReader(
    entrySizeThreshold: Int = GenDarReader.EntrySizeThreshold
) {
  import SupportedFileType._

  /** Reads a DAR from a File. */
  def readFile(file: File): Try[Dar[ArchivePayload]] =
    supportedFileType(file).flatMap {
      case DarFile => readDarStream(file.getName, new ZipInputStream(new FileInputStream(file)))
      case DalfFile => readDalfStream(new FileInputStream(file))
    }

  /** Reads a DAR from an InputStream. This method takes care of closing the stream! */
  def readDarStream(fileName: String, dar: ZipInputStream): Try[Dar[ArchivePayload]] =
    bracket(Try(dar))(dar => Try(dar.close()))
      .flatMap(DarReader.readArchive(fileName, _, entrySizeThreshold))

  /** Reads a DALF from an InputStream. This method takes care of closing the stream! */
  def readDalfStream(dalf: InputStream): Try[Dar[ArchivePayload]] =
    bracket(Try(dalf))(dalf => Try(dalf.close()))
      .flatMap(is => Try(Reader.readArchive(is)))
      .map(Dar(_, List.empty))

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
