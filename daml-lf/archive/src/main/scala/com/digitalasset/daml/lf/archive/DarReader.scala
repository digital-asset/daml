// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.{BufferedInputStream, InputStream}
import java.util.zip.{ZipEntry, ZipFile}

import com.digitalasset.daml.lf.{Dar, DarManifestReader}
import com.digitalasset.daml.lf.data.TryOps.sequence
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class DarReader[A](
    readDalfNamesFromManifest: InputStream => Try[Dar[String]],
    parseDalf: InputStream => Try[A]) {

  private val manifestEntry = new ZipEntry("META-INF/MANIFEST.MF")

  def readArchive(darFile: ZipFile): Try[Dar[A]] =
    for {
      names <- readDalfNames(darFile): Try[Dar[String]]
      main <- parseOne(darFile)(names.main): Try[A]
      deps <- parseAll(darFile)(names.dependencies): Try[List[A]]
    } yield Dar(main, deps)

  private def readDalfNames(darFile: ZipFile): Try[Dar[String]] = {
    val entries: List[ZipEntry] = darFile.entries.asScala.toList

    if (entries.count(e => e.getName == manifestEntry.getName) == 0)
      findDalfNames(entries)
    else
      parseDalfNamesFromManifest(darFile)
  }

  private def parseDalfNamesFromManifest(darFile: ZipFile): Try[Dar[String]] =
    bracket(Try(darFile.getInputStream(manifestEntry)))(close)
      .flatMap(is => readDalfNamesFromManifest(is))

  private def findDalfNames(entries: List[ZipEntry]): Try[Dar[String]] = {
    val dalfs: List[String] = entries.filter(isDalf).map(_.getName)
    dalfs.partition(isPrimDalf) match {
      case (List(prim), List(main)) => Success(Dar(main, List(prim)))
      case (List(prim), List()) => Success(Dar(prim, List.empty))
      case (List(), List(main)) => Success(Dar(main, List.empty))
      case _ => Failure(InvalidLegacyDarFormat(dalfs))
    }
  }

  private def isDalf(e: ZipEntry): Boolean = isDalf(e.getName)

  private def isDalf(s: String): Boolean = s.toLowerCase.endsWith(".dalf")

  private def isPrimDalf(s: String): Boolean = s.toLowerCase.contains("-prim") && isDalf(s)

  private def parseAll(f: ZipFile)(names: List[String]): Try[List[A]] =
    sequence(names.map(parseOne(f)))

  private def parseOne(f: ZipFile)(s: String): Try[A] =
    bracket(getZipEntryInputStream(f, s))(close).flatMap(parseDalf)

  private def getZipEntryInputStream(f: ZipFile, name: String): Try[InputStream] =
    for {
      e <- Try(new ZipEntry(name))
      is <- Try(new BufferedInputStream(f.getInputStream(e)))
    } yield is

  private def close(is: InputStream): Try[Unit] = Try(is.close())
}

case class InvalidLegacyDarFormat(dalfNames: List[String])
    extends RuntimeException(
      s"Invalid Legacy DAR. Legacy DAR can contain: one main DALF and an optional *-prim* DALF." +
        s" This DAR contains: [${dalfNames.mkString(", "): String}]")

object DarReader {
  def apply(): DarReader[(Ref.PackageId, DamlLf.ArchivePayload)] =
    new DarReader(DarManifestReader.dalfNames, a => Try(Reader.decodeArchiveFromInputStream(a)))

  def apply[A](parseDalf: InputStream => Try[A]): DarReader[A] =
    new DarReader(DarManifestReader.dalfNames, parseDalf)
}

object DarReaderWithVersion
    extends DarReader[((Ref.PackageId, DamlLf.ArchivePayload), LanguageMajorVersion)](
      DarManifestReader.dalfNames,
      a => Try(Reader.readArchiveAndVersion(a)))
