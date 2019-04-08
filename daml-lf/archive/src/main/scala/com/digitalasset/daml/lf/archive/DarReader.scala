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

import scala.util.Try

class DarReader[A](
    readDalfNamesFromManifest: InputStream => Try[Dar[String]],
    parseDalf: InputStream => Try[A]) {

  private val manifestEntry = new ZipEntry("META-INF/MANIFEST.MF")

  def readArchive(darFile: ZipFile): Try[Dar[A]] =
    for {
      names <- parseDalfNamesFromManifest(darFile): Try[Dar[String]]
      main <- parseOne(darFile)(names.main): Try[A]
      deps <- parseAll(darFile)(names.dependencies): Try[List[A]]
    } yield Dar(main, deps)

  private def parseDalfNamesFromManifest(darFile: ZipFile): Try[Dar[String]] =
    bracket(Try(darFile.getInputStream(manifestEntry)))(close)
      .flatMap(is => readDalfNamesFromManifest(is))

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
