// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io._
import java.util.zip.ZipInputStream

import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageMajorVersion
import com.daml.daml_lf_dev.DamlLf

import scala.util.{Failure, Success, Try}

import com.daml.lf.data.TryOps.Bracket.bracket

/**
  * Can parse DARs and DALFs.
  * See factories:
  * [[com.daml.lf.archive.UniversalArchiveReader]];
  * [[com.daml.lf.archive.UniversalArchiveReaderWithVersion]]
  *
  * @param parseDar  function to parse a DAR file.
  * @param parseDalf function to parse a DALF input stream.
  * @tparam A type of the result, see factories for more details.
  */
class UniversalArchiveReader[A](
    parseDar: (String, ZipInputStream) => Try[Dar[A]],
    parseDalf: InputStream => Try[A]) {

  import SupportedFileType._

  /** Reads a DAR from a File. */
  def readFile(file: File): Try[Dar[A]] =
    supportedFileType(file).flatMap {
      case DarFile => readDarStream(file.getName, new ZipInputStream(new FileInputStream(file)))
      case DalfFile => readDalfStream(new FileInputStream(file))
    }

  /** Reads a DAR from an InputStream. This method takes care of closing the stream! */
  def readDarStream(fileName: String, dar: ZipInputStream): Try[Dar[A]] =
    bracket(Try(dar))(dar => Try(dar.close())).flatMap(parseDar(fileName, _))

  /** Reads a DALF from an InputStream. This method takes care of closing the stream! */
  def readDalfStream(dalf: InputStream): Try[Dar[A]] =
    bracket(Try(dalf))(dalf => Try(dalf.close())).flatMap(parseDalf).map(Dar(_, List.empty))

}

/**
  * Factory for [[com.daml.lf.archive.UniversalArchiveReader]] class.
  */
object UniversalArchiveReader {
  def apply(entrySizeThreshold: Int = DarReader.EntrySizeThreshold)
    : UniversalArchiveReader[(Ref.PackageId, DamlLf.ArchivePayload)] =
    new UniversalArchiveReader(parseDar(entrySizeThreshold, parseDalf), parseDalf)

  def apply[A](
      entrySizeThreshold: Int,
      parseDalf: InputStream => Try[A]): UniversalArchiveReader[A] =
    new UniversalArchiveReader[A](parseDar(entrySizeThreshold, parseDalf), parseDalf)

  def apply[A](parseDalf: InputStream => Try[A]): UniversalArchiveReader[A] =
    new UniversalArchiveReader[A](parseDar(DarReader.EntrySizeThreshold, parseDalf), parseDalf)

  private def parseDalf(is: InputStream) = Try(Reader.decodeArchiveFromInputStream(is))

  private def parseDar[A](
      entrySizeThreshold: Int,
      parseDalf: InputStream => Try[A],
  ): (String, ZipInputStream) => Try[Dar[A]] =
    DarReader { case (_, is) => parseDalf(is) }.readArchive(_, _, entrySizeThreshold)
}

/**
  * Factory for [[com.daml.lf.archive.UniversalArchiveReader]] class.
  */
object UniversalArchiveReaderWithVersion {
  def apply()
    : UniversalArchiveReader[((Ref.PackageId, DamlLf.ArchivePayload), LanguageMajorVersion)] =
    UniversalArchiveReader(parseDalf)

  private def parseDalf(is: InputStream) = Try(Reader.readArchiveAndVersion(is))
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
