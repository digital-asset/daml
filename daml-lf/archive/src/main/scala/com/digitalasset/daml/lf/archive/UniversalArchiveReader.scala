// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import java.io._
import java.util.zip.ZipFile

import com.digitalasset.daml.lf.archive.{DarReader, LanguageMajorVersion, Reader}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket
import com.digitalasset.daml_lf.DamlLf

import scala.util.{Failure, Success, Try}

/**
  * Can parse DARs and DALFs.
  * See factories:
  *   [[com.digitalasset.daml.lf.UniversalArchiveReader]];
  *   [[com.digitalasset.daml.lf.UniversalArchiveReaderWithVersion]]
  *
  * @param parseDar  function to parse a DAR file.
  * @param parseDalf function to parse a DALF input stream.
  * @tparam A type of the result, see factories for more details.
  */
class UniversalArchiveReader[A](
    parseDar: ZipFile => Try[Dar[A]],
    parseDalf: InputStream => Try[A]) {
  import SupportedFileType._

  def readArchive(file: File): Try[Dar[A]] = supportedFileType(file).flatMap {
    case DarFile =>
      bracket(zipFile(file))(close).flatMap(parseDar)
    case DalfFile =>
      bracket(inputStream(file))(close).flatMap(parseDalf).map(Dar(_, List.empty))
  }

  private def zipFile(f: File): Try[ZipFile] = Try(new ZipFile(f))

  private def inputStream(f: File): Try[InputStream] =
    Try(new BufferedInputStream(new FileInputStream(f)))

  private def close(f: Closeable): Try[Unit] = Try(f.close())
}

/**
  * Factory for [[com.digitalasset.daml.lf.UniversalArchiveReader]] class.
  */
object UniversalArchiveReader {
  def apply(): UniversalArchiveReader[(Ref.PackageId, DamlLf.ArchivePayload)] =
    new UniversalArchiveReader(parseDar(parseDalf), parseDalf)

  def apply[A](parseDalf: InputStream => Try[A]): UniversalArchiveReader[A] =
    new UniversalArchiveReader[A](parseDar(parseDalf), parseDalf)

  private def parseDalf(is: InputStream) = Try(Reader.decodeArchiveFromInputStream(is))

  private def parseDar[A](parseDalf: InputStream => Try[A]): ZipFile => Try[Dar[A]] =
    DarReader(parseDalf).readArchive
}

/**
  * Factory for [[com.digitalasset.daml.lf.UniversalArchiveReader]] class.
  */
object UniversalArchiveReaderWithVersion {
  def apply()
    : UniversalArchiveReader[((Ref.PackageId, DamlLf.ArchivePayload), LanguageMajorVersion)] =
    UniversalArchiveReader(parseDalf)

  private def parseDalf(is: InputStream) = Try(Reader.readArchiveAndVersion(is))
}

private[lf] object SupportedFileType {
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
