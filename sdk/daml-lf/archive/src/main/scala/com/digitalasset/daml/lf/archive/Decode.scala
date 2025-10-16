// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, Util => AstUtil}

object Decode {

  /*
   * Decodes an ArchivePayload into a complete AST.
   * Returns an error if the payload is invalid or contains an unknown LF version.
   */
  def decodeArchivePayload(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.Package)] =
    decodeArchivePayload(payload, onlySchema = false)

  /*
   * Decodes an ArchivePayload into a serializable schema, a partial AST containing
   * only templates, interfaces, and serializable data type definitions, ignoring
   * any other definition and the embedded expressions within those definitions.
   * Ignores the package patch version but returns an error if anything it reads is invalid.
   */
  def decodeArchivePayloadSchema(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    decodeArchivePayload(payload, onlySchema = true)
      .map { case (pkgId, pkg) => pkgId -> AstUtil.toSignature(pkg) }

  // decode an ArchivePayload
  private[this] def decodeArchivePayload(
      payload: ArchivePayload,
      onlySchema: Boolean,
  ): Either[Error, (PackageId, Ast.Package)] =
    payload match {
      case ArchivePayload.Lf2(pkgId, protoPkg, minor, patch)
          if LanguageMajorVersion.V2.supportedMinorVersions.contains(minor) =>
        new DecodeV2(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            onlySchema,
            patch,
          )
          .map(payload.pkgId -> _)
      case ArchivePayload.Lf1(pkgId, protoPkg, minor)
          if LanguageMajorVersion.V1.supportedMinorVersions.contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            onlySchema,
          )
          .map(payload.pkgId -> _)
      case _ =>
        Left(Error.Parsing(s"${payload.version} unsupported"))
    }

  @throws[Error]
  def assertDecodeArchivePayload(
      payload: ArchivePayload,
      onlySchema: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchivePayload(payload, onlySchema = onlySchema))

  // decode an Archive
  def decodeArchive(
      archive: DamlLf.Archive,
      onlySchema: Boolean = false,
  ): Either[Error, (PackageId, Ast.Package)] =
    Reader
      .readArchive(archive, onlySchema)
      .flatMap(decodeArchivePayload(_, onlySchema = onlySchema))

  @throws[Error]
  def assertDecodeArchive(
      archive: DamlLf.Archive,
      onlySchema: Boolean = false,
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchive(archive, onlySchema = onlySchema))

}
