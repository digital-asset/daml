// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}

object Decode {

  // decode an ArchivePayload
  def decodeArchivePayload(
      payload: ArchivePayload,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    payload.version match {
      case LanguageVersion(LanguageMajorVersion.V1, minor)
          if LanguageMajorVersion.V1.supportedMinorVersions.contains(minor) =>
        payload.pkgId ->
          new DecodeV1(minor).decodePackage(
            payload.pkgId,
            payload.proto.getDamlLf1,
            onlySerializableDataDefs,
          )
      case v => throw Error.Parsing(s"$v unsupported")
    }

  // decode an Archive
  def decodeArchive(
      archive: DamlLf.Archive,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    decodeArchivePayload(Reader.readArchive(archive), onlySerializableDataDefs)

}
