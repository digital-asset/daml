// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.typesig.reader

import com.daml.lf.archive
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.daml_lf_dev.DamlLf
import scalaz.\/

object DamlLfArchiveReader {

  private[this] def fromEither[X](either: Either[archive.Error, X]) =
    \/.fromEither(either).leftMap(err => s"Cannot parse archive: $err")

  def readPackage(lf: DamlLf.Archive): String \/ (Ref.PackageId, Ast.Package) =
    fromEither(archive.Reader.readArchive(lf)) flatMap readPackage

  def readPackage(
      packageId: Ref.PackageId,
      lf: DamlLf.ArchivePayload,
  ): String \/ (Ref.PackageId, Ast.Package) =
    fromEither(archive.Reader.readArchivePayload(packageId, lf)) flatMap readPackage

  private[typesig] def readPackage(
      payLoad: archive.ArchivePayload
  ): String \/ (Ref.PackageId, Ast.Package) =
    fromEither(archive.Decode.decodeArchivePayload(payLoad, onlySerializableDataDefs = true))

}
