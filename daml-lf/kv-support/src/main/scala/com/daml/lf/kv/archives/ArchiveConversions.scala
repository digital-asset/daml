// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.archives

import com.daml.SafeProto
import com.daml.lf.archive.{ArchiveParser, Decode, Error => ArchiveError}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast

object ArchiveConversions {

  def parsePackageId(rawArchive: RawArchive): Either[ArchiveError, Ref.PackageId] =
    for {
      archive <- ArchiveParser.fromByteString(rawArchive.byteString)
      packageId <- Ref.PackageId
        .fromString(archive.getHash)
        .left
        .map(ArchiveError.Parsing)
    } yield packageId

  def parsePackageIdsAndRawArchives(
      archives: List[com.daml.daml_lf_dev.DamlLf.Archive]
  ): Either[ArchiveError, Map[Ref.PackageId, RawArchive]] =
    archives.partitionMap { archive =>
      for {
        pkgId <- Ref.PackageId.fromString(archive.getHash).left.map(ArchiveError.Parsing)
        bytes <- SafeProto.toByteString(archive).left.map(ArchiveError.Encoding)
      } yield pkgId -> RawArchive(bytes)
    } match {
      case (Nil, hashesAndRawArchives) => Right(hashesAndRawArchives.toMap)
      case (errors, _) => Left(errors.head)
    }

  def decodePackages(
      hashesAndArchives: Iterable[RawArchive]
  ): Either[ArchiveError, Map[Ref.PackageId, Ast.Package]] = {
    type Result = Either[ArchiveError, Map[Ref.PackageId, Ast.Package]]
    hashesAndArchives
      .foldLeft[Result](Right(Map.empty)) { (acc, rawArchive) =>
        for {
          result <- acc
          packageAst <- decodePackage(rawArchive)
        } yield result + packageAst
      }
  }

  def decodePackage(
      rawArchive: RawArchive
  ): Either[ArchiveError, (PackageId, Ast.Package)] =
    ArchiveParser
      .fromByteString(rawArchive.byteString)
      .flatMap(archive => Decode.decodeArchive(archive))
}
