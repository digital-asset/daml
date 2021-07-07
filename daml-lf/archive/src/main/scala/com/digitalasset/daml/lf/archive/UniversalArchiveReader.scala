// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.{File, FileInputStream, InputStream}
import java.util.zip.ZipInputStream

import scala.util.{Failure, Success, Try}
import com.daml.lf.data.TryOps.Bracket.bracket

/** Can parse DARs and DALFs.
  *
  * @param parseDar  function to parse a DAR file.
  * @param parseDalf function to parse a DALF input stream.
  */
class UniversalArchiveReader(
    parseDar: (String, ZipInputStream) => Try[Dar[ArchivePayload]],
    parseDalf: InputStream => Try[ArchivePayload],
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
    bracket(Try(dar))(dar => Try(dar.close())).flatMap(parseDar(fileName, _))

  /** Reads a DALF from an InputStream. This method takes care of closing the stream! */
  def readDalfStream(dalf: InputStream): Try[Dar[ArchivePayload]] =
    bracket(Try(dalf))(dalf => Try(dalf.close())).flatMap(parseDalf).map(Dar(_, List.empty))

}

/** Factory for [[com.daml.lf.archive.UniversalArchiveReader]] class.
  */
object UniversalArchiveReader {
  def apply(
      entrySizeThreshold: Int = DarReader.EntrySizeThreshold
  ): UniversalArchiveReader =
    new UniversalArchiveReader(parseDar(entrySizeThreshold, parseDalf), parseDalf)

  def apply(
      entrySizeThreshold: Int,
      parseDalf: InputStream => Try[ArchivePayload],
  ): UniversalArchiveReader =
    new UniversalArchiveReader(parseDar(entrySizeThreshold, parseDalf), parseDalf)

  def apply(parseDalf: InputStream => Try[ArchivePayload]): UniversalArchiveReader =
    new UniversalArchiveReader(parseDar(DarReader.EntrySizeThreshold, parseDalf), parseDalf)

  private def parseDalf(is: InputStream) = Try(Reader.readArchive(is))

  private def parseDar[A](
      entrySizeThreshold: Int,
      parseDalf: InputStream => Try[A],
  ): (String, ZipInputStream) => Try[Dar[A]] =
    DarReader[A] { case (_, is) => parseDalf(is) }.readArchive(_, _, entrySizeThreshold)
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
