// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive.testing

import java.security.MessageDigest

import com.daml.daml_lf.ArchiveOuterClass.Archive
import com.daml.daml_lf_dev.{DamlLf => PLF}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

// Important: do not use this in production code. It is designed for testing only.
object Encode {

  private def encodePayloadOfVersion(
      idAndPkg: (PackageId, Package),
      version: LanguageVersion,
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

  final def encodeArchive(pkg: (PackageId, Package), version: LanguageVersion): Archive = {

    val payload = encodePayloadOfVersion(pkg, version).toByteString
    val hash = PackageId.assertFromString(
      MessageDigest.getInstance("SHA-256").digest(payload.toByteArray).map("%02x" format _).mkString
    )

    Archive
      .newBuilder()
      .setHashFunction(Archive.HashFunction.SHA256)
      .setPayload(payload)
      .setHash(hash)
      .build()

  }

  case class EncodeError(message: String) extends RuntimeException

  private[testing] def unexpectedError(): Unit = throw EncodeError("unexpected error")

  private[testing] def expect(b: Boolean): Unit = if (!b) unexpectedError()
}
