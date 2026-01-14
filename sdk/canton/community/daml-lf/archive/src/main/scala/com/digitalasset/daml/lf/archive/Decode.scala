// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}

object Decode {

  /*
   * Decodes an ArchivePayload into a complete AST.
   * Returns an error if the payload is invalid or contains an unknown LF version.
   */
  def decodeArchivePayload(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.Package)] =
    decodeArchivePayload(payload, schemaMode = false)

  @throws[Error]
  def assertDecodeArchivePayload(
      payload: ArchivePayload
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchivePayload(payload))

  /*
   * Decodes an ArchivePayload into a serializable schema, a partial AST containing
   * only templates, interfaces, and serializable data type definitions, ignoring
   * any other definition and the embedded expressions within those definitions.
   * Ignores the package patch version but returns an error if anything it reads is invalid.
   */
  def decodeArchivePayloadSchema(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    decodeArchivePayload(payload, schemaMode = true)
      .map { case (pkgId, pkg) => pkgId -> AstUtil.toSignature(pkg) }

  @throws[Error]
  def assertDecodeArchivePayloadSchema(
      payload: ArchivePayload
  ): (PackageId, Ast.PackageSignature) =
    assertRight(decodeArchivePayloadSchema(payload))

  private[this] def decodeArchivePayload(
      payload: ArchivePayload,
      schemaMode: Boolean,
  ): Either[Error, (PackageId, Ast.Package)] = {
    payload match {
      case ArchivePayload.Lf2(pkgId, protoPkg, minor, patch)
          if LanguageVersion.allLfVersions.map(_.minor).contains(minor) =>
        new DecodeV2(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
            patch,
          )
          .map(payload.pkgId -> _)
      case ArchivePayload.Lf1(pkgId, protoPkg, minor)
          if LanguageVersion.allLegacyLfVersions.map(_.minor).contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
          )
          .map(payload.pkgId -> _)
      case _ =>
        Left(
          Error.Parsing(s"Encountered unsupported LF version ${payload.version} during decoding")
        )
    }
  }

  def decodeArchive(
      archive: DamlLf.Archive
  ): Either[Error, (PackageId, Ast.Package)] =
    Reader
      .readArchive(archive, schemaMode = false)
      .flatMap(decodeArchivePayload)

  @throws[Error]
  def assertDecodeArchive(
      archive: DamlLf.Archive
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchive(archive))

  def decodeArchiveSchema(
      archive: DamlLf.Archive
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    Reader
      .readArchive(archive, schemaMode = true)
      .flatMap(decodeArchivePayloadSchema)

  @throws[Error]
  def assertDecodeArchiveSchema(
      archive: DamlLf.Archive
  ): (PackageId, Ast.PackageSignature) =
    assertRight(decodeArchiveSchema(archive))

}
