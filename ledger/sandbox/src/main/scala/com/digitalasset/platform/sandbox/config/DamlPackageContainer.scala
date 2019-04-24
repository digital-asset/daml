// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.zip.ZipFile

import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.daml_lf.DamlLf.Archive

import scala.collection.breakOut
import scala.collection.immutable.Iterable
import scala.util.control.NonFatal
import scala.util.Try

case class DamlPackageContainer(files: List[File] = Nil, devAllowed: Boolean = false) {

  lazy val archives: List[Archive] =
    files.flatMap { file =>
      val fileName = file.getName
      if (fileName.endsWith(".dalf"))
        archivesFromDalf(file)
      else if (fileName.endsWith(".dar")) {
        archivesFromDar(file)
      } else {
        sys.error(
          s"Expected DAML archives with .dalf or .dar extension. '$file' does not fit that pattern.")
      }
    }

  private def archivesFromDar(file: File): List[Archive] = {
    DarReader[Archive](x => Try(Archive.parseFrom(x)))
      .readArchive(new ZipFile(file))
      .fold(t => throw new RuntimeException(s"Failed to parse DAR from $file", t), dar => dar.all)
  }

  private def archivesFromDalf(file: File): List[Archive] = {
    var is: BufferedInputStream = null
    if (!file.canRead) {
      sys.error(s"DAML archive ${file} does not exist or is not readable.")
    }
    try {
      is = new BufferedInputStream(new FileInputStream(file))
      List(Archive.parseFrom(is))
    } catch {
      case NonFatal(t) => sys.error(s"Failed to parse DALF from ${file}: $t")
    } finally {
      is.close()
    }
  }

  lazy val packages: Map[PackageId, Ast.Package] = {
    val decode: Decode = if (devAllowed) {
      Decode.WithDevSupport
    } else Decode
    archives.map(decode.decodeArchive)(breakOut)
  }

  lazy val packageIds: Iterable[String] = archives.map(_.getHash)

  def withFile(file: File): DamlPackageContainer = copy(files = file :: files)

  def getPackage(id: PackageId): Option[Ast.Package] = packages.get(id)

  def allowDev: DamlPackageContainer = copy(devAllowed = true)
}
