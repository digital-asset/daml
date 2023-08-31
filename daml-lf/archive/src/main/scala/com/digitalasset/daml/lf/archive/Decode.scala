// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.{DamlLf, DamlLf1, DamlLf2}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.util.PackageInfo
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.CodedInputStream

object Decode {

  // decode an ArchivePayload
  def decodeArchivePayload(
      payload: ArchivePayload,
      onlySerializableDataDefs: Boolean = false,
  ): Either[Error, (PackageId, Ast.Package)] =
    payload.version match {
      case LanguageVersion(LanguageMajorVersion.V1, minor)
          if LanguageMajorVersion.V1.supportedMinorVersions.contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            payload.pkgId,
            payload.proto.getDamlLf1,
            onlySerializableDataDefs,
          )
          .map(payload.pkgId -> _)
      case LanguageVersion(LanguageMajorVersion.V2, minor)
          if LanguageMajorVersion.V2.supportedMinorVersions.contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            payload.pkgId,
            coerce(payload.proto.getDamlLf2),
            onlySerializableDataDefs,
          )
          .map(payload.pkgId -> _)
      case v => Left(Error.Parsing(s"$v unsupported"))
    }

  // For the moment, v1 and v2 are wire compatible
  // TODO(paul): move this to archive/package.scala to share the 1000 constant
  private def coerce(lf2Package: DamlLf2.Package): DamlLf1.Package = {
    val codedInputStream = CodedInputStream.newInstance(lf2Package.toByteArray)
    discard(codedInputStream.setRecursionLimit(1000))
    DamlLf1.Package.parseFrom(codedInputStream)
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

  def decodeInfoPackage(archive: DamlLf.Archive): Either[Error, PackageInfo] =
    decodeArchive(archive, onlySerializableDataDefs = true)
      .map(entry => new PackageInfo(Map(entry)))

  def assertDecodeInfoPackage(archive: DamlLf.Archive): PackageInfo =
    assertRight(decodeInfoPackage(archive: DamlLf.Archive))

}
