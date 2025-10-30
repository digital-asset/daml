// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, Util => AstUtil}

object Decode {

  def decodeArchivePayload(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.Package)] =
    decodeArchivePayload(payload, schemaMode = false)

  def decodeArchivePayloadSchema(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    decodeArchivePayload(payload, schemaMode = true).map { case (pkgId, pkg) =>
      pkgId -> AstUtil.toSignature(pkg)
    }

  // decode an ArchivePayload
  private[this] def decodeArchivePayload(
      payload: ArchivePayload,
      schemaMode: Boolean,
  ): Either[Error, (PackageId, Ast.Package)] =
    payload match {
      case ArchivePayload.Lf2(pkgId, protoPkg, minor, patch)
          if LanguageMajorVersion.V2.supportedMinorVersions.contains(minor) =>
        new DecodeV2(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
            patch,
          )
          .map(payload.pkgId -> _)
      case ArchivePayload.Lf1(pkgId, protoPkg, minor)
          if LanguageMajorVersion.V1.supportedMinorVersions.contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
          )
          .map(payload.pkgId -> _)
      case _ =>
        Left(Error.Parsing(s"${payload.version} unsupported"))
    }

  @throws[Error]
  def assertDecodeArchivePayload(
      payload: ArchivePayload,
      schemaMode: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchivePayload(payload, schemaMode = schemaMode))

  // decode an Archive
  def decodeArchive(
      archive: DamlLf.Archive,
      schemaMode: Boolean = false,
  ): Either[Error, (PackageId, Ast.Package)] =
    Reader
      .readArchive(archive)
      .flatMap(decodeArchivePayload(_, schemaMode = schemaMode))

  @throws[Error]
  def assertDecodeArchive(
      archive: DamlLf.Archive,
      schemaMode: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchive(archive, schemaMode = schemaMode))

}
