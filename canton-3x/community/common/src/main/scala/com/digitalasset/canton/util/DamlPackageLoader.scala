// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.*
import com.daml.lf.archive.{DarDecoder, DarReader}
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.google.protobuf.ByteString

import java.io.{ByteArrayInputStream, File, InputStream}
import java.util.zip.ZipInputStream

/** Wrapper that retrieves parsed packages from a DAR file consumable by the Daml interpreter.
  */
object DamlPackageLoader {

  def getPackagesFromDarFile(filename: String): Either[LoadError, Map[Ref.PackageId, Ast.Package]] =
    getPackagesFromDarFiles(Seq(filename))

  def getPackagesFromDarFiles(
      filenames: Seq[String]
  ): Either[LoadError, Map[Ref.PackageId, Ast.Package]] = {
    filenames
      .traverse { filename =>
        DarDecoder
          .readArchiveFromFile(new File(filename))
          .bimap(t => LoadError(t.toString), _.all.toMap)
      }
      .map(_.foldLeft(Map.empty[Ref.PackageId, Ast.Package])(_ ++ _))
  }

  def getPackagesFromInputStream(
      name: String,
      inputStream: InputStream,
  ): Either[LoadError, Map[Ref.PackageId, Ast.Package]] =
    DarDecoder
      .readArchive(name, new ZipInputStream(inputStream))
      .bimap(t => LoadError(t.toString), _.all.toMap)

  final case class LoadError(message: String)

  /** Utility to check that the DAR file is valid.
    * This will check useful things such as whether the DAR file is potentially a zipbomb and that it
    * follows the expected specification.
    * By specifying unzippedMaxBytes, it will check that the unzipped DAR does not exceed this size. Defaults to 1GB.
    */
  def validateDar(
      name: String,
      content: ByteString,
      unzippedMaxBytes: Int = 1064 * 1064 * 1064,
  ): Either[String, Unit] =
    DarReader
      .readArchive(
        name,
        new ZipInputStream(new ByteArrayInputStream(content.toByteArray)),
        unzippedMaxBytes,
      )
      .bimap(t => t.getMessage, _ => ())

}
