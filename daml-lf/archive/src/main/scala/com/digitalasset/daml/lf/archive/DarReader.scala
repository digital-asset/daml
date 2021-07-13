// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Bytes
import com.daml.lf.data.TryOps.sequence

import java.io.{File, FileInputStream, IOException, InputStream}
import java.util.zip.ZipInputStream
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try, Using}

class GenDarReader[A](parseDalf: Bytes => Try[A]) {

  import GenDarReader._

  /** Reads an archive from a File. */
  def readArchiveFromFile(darFile: File): Try[Dar[A]] =
    Using(new ZipInputStream(new FileInputStream(darFile)))(readArchive(darFile.getName, _)).flatten

  /** Reads an archive from a ZipInputStream. The stream will be closed by this function! */
  def readArchive(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int = EntrySizeThreshold,
  ): Try[Dar[A]] =
    for {
      entries <- loadZipEntries(name, darStream, entrySizeThreshold)
      names <- entries.readDalfNames
      main <- parseOne(entries.get)(names.main)
      deps <- parseAll(entries.get)(names.dependencies)
    } yield Dar(main, deps)

  // Fails if a zip bomb is detected
  @throws[Error.ZipBomb]
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
      if (output.length >= entrySizeThreshold) throw Error.ZipBomb()
    }
    name -> output
  }

  private[this] def loadZipEntries(
      name: String,
      darStream: ZipInputStream,
      entrySizeThreshold: Int,
  ): Try[ZipEntries] =
    Try(
      Iterator
        .continually(darStream.getNextEntry)
        .takeWhile(_ != null)
        .map(entry => slurpWithCaution(entry.getName, darStream, entrySizeThreshold))
        .toMap
    ).map(ZipEntries(name, _))

  private[this] def parseAll(getPayload: String => Try[Bytes])(names: List[String]): Try[List[A]] =
    sequence(names.map(parseOne(getPayload)))

  private[this] def parseOne(getPayload: String => Try[Bytes])(s: String): Try[A] =
    getPayload(s).flatMap(parseDalf)

}

object GenDarReader {

  private val ManifestName = "META-INF/MANIFEST.MF"
  private[archive] val EntrySizeThreshold = 1024 * 1024 * 1024 // 1 GB

  private[archive] case class ZipEntry(size: Long, getStream: () => InputStream)

  private[archive] case class ZipEntries(name: String, entries: Map[String, Bytes]) {
    private[GenDarReader] def get(entryName: String): Try[Bytes] = {
      entries.get(entryName) match {
        case Some(is) => Success(is)
        case None => Failure(Error.InvalidZipEntry(entryName, this))
      }
    }

    private[GenDarReader] def readDalfNames: Try[Dar[String]] =
      get(ManifestName)
        .flatMap(DarManifestReader.dalfNames)
        .recoverWith { case NonFatal(e1) => Failure(Error.InvalidDar(this, e1)) }
  }
}

object DarReader extends GenDarReader[ArchivePayload](is => Try(Reader.readArchive(is)))

object RawDarReader
    extends GenDarReader[DamlLf.Archive](is => Try(DamlLf.Archive.parseFrom(is.toByteString)))
