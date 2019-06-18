// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.archive

import java.security.MessageDigest

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml_lf.{DamlLf => PLF}

// Important: do not use this in production code. It is designed for testing only.
private[digitalasset] object Encode {

  private def encodePayloadOfVersion(
      idAndPkg: (PackageId, Package),
      version: LanguageVersion
  ): PLF.ArchivePayload = {

    val (pkgId, pkg) = idAndPkg
    val LanguageVersion(major, minor) = version

    major match {
      case LanguageMajorVersion.V1 =>
        PLF.ArchivePayload
          .newBuilder()
          .setMinor(minor.toProtoIdentifier)
          .setDamlLf1(new EncodeV1(minor).encodePackage(pkgId, pkg))
          .build()
      case _ =>
        sys.error(s"$version not supported")
    }
  }

  final def encodeArchive(pkg: (PackageId, Package), version: LanguageVersion): PLF.Archive = {

    val payload = encodePayloadOfVersion(pkg, version).toByteString
    val hash = PackageId.assertFromString(
      MessageDigest.getInstance("SHA-256").digest(payload.toByteArray).map("%02x" format _).mkString
    )

    PLF.Archive
      .newBuilder()
      .setHashFunction(PLF.HashFunction.SHA256)
      .setPayload(payload)
      .setHash(hash)
      .build()

  }

}
