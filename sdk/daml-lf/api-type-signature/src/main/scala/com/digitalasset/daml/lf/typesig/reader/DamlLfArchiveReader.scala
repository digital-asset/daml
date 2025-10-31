// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package typesig
package reader

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.archive.DamlLf
import scalaz.\/

object DamlLfArchiveReader {

  private[this] def fromEither[X](either: Either[archive.Error, X]) =
    \/.fromEither(either).leftMap(err => s"Cannot parse archive: $err")

  def readPackage(lf: DamlLf.Archive): String \/ (Ref.PackageId, Ast.PackageSignature) =
    fromEither(archive.Reader.readArchive(lf, schemaMode = true)) flatMap readPackage

  def readPackage(
      packageId: Ref.PackageId,
      lf: DamlLf.ArchivePayload,
  ): String \/ (Ref.PackageId, Ast.PackageSignature) =
    fromEither(
      archive.Reader.readArchivePayload(packageId, lf, schemaMode = true)
    ) flatMap readPackage

  private[typesig] def readPackage(
      payLoad: archive.ArchivePayload
  ): String \/ (Ref.PackageId, Ast.PackageSignature) =
    fromEither(archive.Decode.decodeArchivePayloadSchema(payLoad))

}
