// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.archives

import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.lf.kv.ConversionError

object ArchiveConversions {

  def parsePackageId(rawArchive: RawArchive): Either[ConversionError.ParseError, Ref.PackageId] =
    for {
      archive <- ArchiveParser
        .fromByteString(rawArchive.byteString)
        .left
        .map(error => ConversionError.ParseError(error.msg))
      packageId <- Ref.PackageId
        .fromString(archive.getHash)
        .left
        .map(ConversionError.ParseError)
    } yield packageId

  def parsePackageIdsAndRawArchives(
      archives: List[com.daml.daml_lf_dev.DamlLf.Archive]
  ): Either[ConversionError.ParseError, Map[Ref.PackageId, RawArchive]] =
    archives.partitionMap { archive =>
      Ref.PackageId.fromString(archive.getHash).map(_ -> RawArchive(archive.toByteString))
    } match {
      case (Nil, hashesAndRawArchives) => Right(hashesAndRawArchives.toMap)
      case (errors, _) => Left(ConversionError.ParseError(errors.head))
    }
}
