// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import java.io._
import java.util.zip.ZipInputStream

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml_lf.DamlLf

import scala.util.{Failure, Success, Try}

import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket

/**
  * Can parse DARs and DALFs.
  * See factories:
  * [[com.digitalasset.daml.lf.UniversalArchiveReader]];
  * [[com.digitalasset.daml.lf.UniversalArchiveReaderWithVersion]]
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
    for {
      fileType <- supportedFileType(file)
      inputStream <- fileToInputStream(file)
      dar <- readStream(file.getName, inputStream, fileType)
    } yield dar

  /** Reads a DAR from an InputStream. This method takes care of closing the stream! */
  def readStream(
      fileName: String,
      inputStream: InputStream,
      fileType: SupportedFileType): Try[Dar[A]] =
    fileType match {
      case DarFile =>
        bracket(Try(new ZipInputStream(inputStream)))(zis => Try(zis.close())).flatMap(zis =>
          parseDar(fileName, zis))

      case DalfFile =>
        bracket(Try(inputStream))(is => Try(is.close())).flatMap(
          is => parseDalf(is).map(Dar(_, List.empty))
        )
    }

  private def fileToInputStream(f: File): Try[InputStream] =
    Try(new BufferedInputStream(new FileInputStream(f)))

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

  private def parseDar[A](
      parseDalf: InputStream => Try[A]): (String, ZipInputStream) => Try[Dar[A]] =
    DarReader { case (_, is) => parseDalf(is) }.readArchive
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
