// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io._
import java.util.zip.ZipInputStream

import com.daml.lf.archive.Errors.{InvalidDar, InvalidLegacyDar, InvalidZipEntry}
import com.daml.lf.data.Ref
import com.daml.lf.data.TryOps.Bracket.bracket
import com.daml.lf.data.TryOps.sequence
import com.daml.lf.language.LanguageMajorVersion
import com.daml.daml_lf_dev.DamlLf

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DarReader[A](
    readDalfNamesFromManifest: InputStream => Try[Dar[String]],
    // The `Long` is the dalf size in bytes.
    parseDalf: (Long, InputStream) => Try[A]) {

  import DarReader._

  /** Reads an archive from a File. */
  def readArchiveFromFile(darFile: File): Try[Dar[A]] =
    readArchive(darFile.getName, new ZipInputStream(new FileInputStream(darFile)))

  /** Reads an archive from a ZipInputStream. The stream will be closed by this function! */
  def readArchive(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Try[Dar[A]] = {
    for {
      entries <- bracket(Try(darStream))(zis => Try(zis.close())).flatMap(zis =>
        loadZipEntries(name, zis, entrySizeThreshold))
      names <- entries.readDalfNames(readDalfNamesFromManifest): Try[Dar[String]]
      main <- parseOne(entries.getInputStreamFor)(names.main): Try[A]
      deps <- parseAll(entries.getInputStreamFor)(names.dependencies): Try[List[A]]
    } yield Dar(main, deps)
  }

  // Fails if a zip bomb is detected
  private def slurpWithCaution(
      zip: ZipInputStream,
      entrySizeThreshold: Int,
  ): Try[(Long, InputStream)] =
    Try {
      val output = new ByteArrayOutputStream()
      val buffer = new Array[Byte](4096)
      for (n <- Iterator.continually(zip.read(buffer)).takeWhile(_ >= 0) if n > 0) {
        output.write(buffer, 0, n)
        if (output.size >= entrySizeThreshold) throw Errors.ZipBomb()
      }
      (output.size.toLong, new ByteArrayInputStream(output.toByteArray))
    }

  private def loadZipEntries(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int,
  ): Try[ZipEntries] = {
    @tailrec
    def go(accT: Try[Map[String, (Long, InputStream)]]): Try[Map[String, (Long, InputStream)]] =
      Option(darStream.getNextEntry) match {
        case Some(entry) =>
          go(
            accT.flatMap { acc =>
              bracket(slurpWithCaution(darStream, entrySizeThreshold))(_ =>
                Try(darStream.closeEntry()))
                .map { sizedBytes =>
                  acc + (entry.getName -> sizedBytes)
                }
            }
          )
        case None => accT
      }

    go(Success(Map.empty)).map(ZipEntries(name, _))
  }

  private def parseAll(getInputStreamFor: String => Try[(Long, InputStream)])(
      names: List[String]): Try[List[A]] =
    sequence(names.map(parseOne(getInputStreamFor)))

  private def parseOne(getInputStreamFor: String => Try[(Long, InputStream)])(s: String): Try[A] =
    bracket(getInputStreamFor(s))({ case (_, is) => Try(is.close()) }).flatMap({
      case (size, is) =>
        parseDalf(size, is)
    })

}

object Errors {

  import DarReader.ZipEntries

  final case class InvalidDar(entries: ZipEntries, cause: Throwable)
      extends RuntimeException(s"Invalid DAR: ${darInfo(entries): String}", cause)

  final case class InvalidZipEntry(name: String, entries: ZipEntries)
      extends RuntimeException(
        s"Invalid zip entryName: ${name: String}, DAR: ${darInfo(entries): String}")

  final case class InvalidLegacyDar(entries: ZipEntries)
      extends RuntimeException(s"Invalid Legacy DAR: ${darInfo(entries)}")

  final case class ZipBomb()
      extends RuntimeException(s"An entry is too large, rejected as a possible zip bomb")

  private def darInfo(entries: ZipEntries): String =
    s"${entries.name}, content: [${darFileNames(entries).mkString(", "): String}}]"

  private def darFileNames(entries: ZipEntries): Iterable[String] =
    entries.entries.keys
}

object DarReader {

  private val ManifestName = "META-INF/MANIFEST.MF"
  private[archive] val EntrySizeThreshold = 1024 * 1024 * 1024 // 1 GB

  private[archive] case class ZipEntries(name: String, entries: Map[String, (Long, InputStream)]) {

    def getInputStreamFor(entryName: String): Try[(Long, InputStream)] = {
      entries.get(entryName) match {
        case Some((size, is)) => Success(size -> is)
        case None => Failure(InvalidZipEntry(entryName, this))
      }
    }

    def readDalfNames(
        readDalfNamesFromManifest: InputStream => Try[Dar[String]]): Try[Dar[String]] =
      parseDalfNamesFromManifest(readDalfNamesFromManifest).recoverWith {
        case NonFatal(e1) =>
          findLegacyDalfNames().recoverWith {
            case NonFatal(_) => Failure(InvalidDar(this, e1))
          }
      }

    private def parseDalfNamesFromManifest(
        readDalfNamesFromManifest: InputStream => Try[Dar[String]]): Try[Dar[String]] =
      bracket(getInputStreamFor(ManifestName)) { case (_, is) => Try(is.close()) }
        .flatMap { case (_, is) => readDalfNamesFromManifest(is) }

    // There are three cases:
    // 1. if it's only one .dalf, then that's the main one
    // 2. if it's two .dalfs, where one of them has -prim in the name, the one without -prim is the main dalf.
    // 3. parse error in all other cases
    private def findLegacyDalfNames(): Try[Dar[String]] = {
      val dalfs: List[String] = entries.keys.filter(isDalf).toList

      dalfs.partition(isPrimDalf) match {
        case (List(prim), List(main)) => Success(Dar(main, List(prim)))
        case (List(prim), Nil) => Success(Dar(prim, List.empty))
        case (Nil, List(main)) => Success(Dar(main, List.empty))
        case _ => Failure(InvalidLegacyDar(this))
      }
    }

    private def isDalf(s: String): Boolean = s.toLowerCase.endsWith(".dalf")

    private def isPrimDalf(s: String): Boolean = s.toLowerCase.contains("-prim") && isDalf(s)
  }

  def apply(): DarReader[(Ref.PackageId, DamlLf.ArchivePayload)] =
    new DarReader(DarManifestReader.dalfNames, {
      case (_, is) => Try(Reader.decodeArchiveFromInputStream(is))
    })

  def apply[A](parseDalf: (Long, InputStream) => Try[A]): DarReader[A] =
    new DarReader(DarManifestReader.dalfNames, parseDalf)
}

object DarReaderWithVersion
    extends DarReader[((Ref.PackageId, DamlLf.ArchivePayload), LanguageMajorVersion)](
      DarManifestReader.dalfNames,
      { case (_, is) => Try(Reader.readArchiveAndVersion(is)) })
