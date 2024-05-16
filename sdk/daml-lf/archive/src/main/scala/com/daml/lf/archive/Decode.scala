// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageMajorVersion}

object Decode {

  // decode an ArchivePayload
  def decodeArchivePayload(
      payload: ArchivePayload,
      onlySerializableDataDefs: Boolean = false,
  ): Either[Error, (PackageId, Ast.Package)] =
    payload match {
      case ArchivePayload.Lf2(pkgId, protoPkg, minor)
          if LanguageMajorVersion.V2.supportedMinorVersions.contains(minor) =>
        new DecodeV2(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            onlySerializableDataDefs,
          )
          .map(payload.pkgId -> _)
      case ArchivePayload.Lf1(pkgId, protoPkg, minor)
          if LanguageMajorVersion.V1.supportedMinorVersions.contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            onlySerializableDataDefs,
          )
          .map(payload.pkgId -> _)
      case _ =>
        Left(Error.Parsing(s"${payload.version} unsupported"))
    }

  @throws[Error]
  def assertDecodeArchivePayload(
      payload: ArchivePayload,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchivePayload(payload, onlySerializableDataDefs: Boolean))

  // decode an Archive
  def decodeArchive(
      archive: DamlLf.Archive,
      onlySerializableDataDefs: Boolean = false,
  ): Either[Error, (PackageId, Ast.Package)] =
    Reader.readArchive(archive).flatMap(decodeArchivePayload(_, onlySerializableDataDefs))

  @throws[Error]
  def assertDecodeArchive(
      archive: DamlLf.Archive,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchive(archive, onlySerializableDataDefs))

}
