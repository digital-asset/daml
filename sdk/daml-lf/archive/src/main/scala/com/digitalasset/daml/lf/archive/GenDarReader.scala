// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.lf.data.Bytes
import com.daml.nameof.NameOf

import java.io.{File, FileInputStream, IOException}
import java.util.zip.ZipInputStream

import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.either._

sealed abstract class GenDarReader[A] {
  import GenDarReader._

  def readArchiveFromFile(
      darFile: File,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Either[Error, Dar[A]]

  @throws[Error]
  def assertReadArchiveFromFile(darFile: File): Dar[A] =
    assertRight(readArchiveFromFile(darFile))

  def readArchive(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Either[Error, Dar[A]]
}

private[archive] final class GenDarReaderImpl[A](reader: GenReader[A]) extends GenDarReader[A] {

  import GenDarReader._

  /** Reads an archive from a File. */
  override def readArchiveFromFile(
      darFile: File,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Either[Error, Dar[A]] =
    using(NameOf.qualifiedNameOfCurrentFunc, () => new FileInputStream(darFile))(is =>
      readArchive(darFile.getName, new ZipInputStream(is), entrySizeThreshold)
    )

  /** Reads an archive from a ZipInputStream. The stream will be closed by this function! */
  override def readArchive(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Either[Error, Dar[A]] =
    for {
      entries <- loadZipEntries(name, darStream, entrySizeThreshold)
      names <- entries.readDalfNames
      main <- parseOne(entries.get)(names.main)
      deps <- parseAll(entries.get)(names.dependencies)
    } yield Dar(main, deps)

  // Fails if a zip bomb is detected
  @throws[Error.ZipBomb.type]
  @throws[IOException]
  private[this] def slurpWithCaution(
      name: String,
      zip: ZipInputStream,
      entrySizeThreshold: Int,
  ): (String, Bytes) = {
    val buffSize = 4 * 1024 // 4k
    val buffer = Array.ofDim[Byte](buffSize)
    var output = Bytes.Empty
    Iterator.continually(zip.read(buffer)).takeWhile(_ >= 0).foreach { size =>
      output ++= Bytes.fromByteArray(buffer, 0, size)
      if (output.length >= entrySizeThreshold) throw Error.ZipBomb
    }
    name -> output
  }

  private[this] def loadZipEntries(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int,
  ): Either[Error, ZipEntries] =
    attempt(NameOf.qualifiedNameOfCurrentFunc)(
      ZipEntries(
        name,
        Iterator
          .continually(darStream.getNextEntry)
          .takeWhile(_ != null)
          .map(entry => slurpWithCaution(entry.getName, darStream, entrySizeThreshold))
          .toMap,
      )
    )

  private[this] def parseAll(getPayload: String => Either[Error, Bytes])(
      names: List[String]
  ): Either[Error, List[A]] =
    names.traverse(parseOne(getPayload))

  private[this] def parseOne(
      getPayload: String => Either[Error, Bytes]
  )(s: String): Either[Error, A] =
    getPayload(s).flatMap(reader.fromBytes)

}

object GenDarReader {

  def apply[A](reader: GenReader[A]): GenDarReader[A] = new GenDarReaderImpl[A](reader)

  private val ManifestName = "META-INF/MANIFEST.MF"
  private[archive] val EntrySizeThreshold = 1024 * 1024 * 1024 // 1 GB

  private[archive] case class ZipEntries(name: String, entries: Map[String, Bytes]) {
    private[archive] def get(entryName: String): Either[Error, Bytes] =
      entries.get(entryName).toRight(Error.InvalidZipEntry(entryName, this))

    private[archive] def readDalfNames: Either[Error, Dar[String]] =
      get(ManifestName)
        .flatMap(DarManifestReader.dalfNames)
        .left
        .map(Error.InvalidDar(this, _))
  }
}
