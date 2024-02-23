// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive.testing

import com.daml.SafeProto
import com.daml.crypto.MessageDigestPrototype
import com.daml.daml_lf_dev.{DamlLf => PLF}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

/** The Daml-LF Encoder library can be used to build dar files directly from LF
  * definitions without passing through Damlc.
  *
  * It is designed for testing only and provided without any guarantee.
  * In particular future version can break the API without notice.
  */
// Important: do not use this in production code. It is designed for testing only.
object Encode {

  private def encodePayloadOfVersion(
      idAndPkg: (PackageId, Package),
      version: LanguageVersion,
  ): PLF.ArchivePayload = {

    val (pkgId, pkg) = idAndPkg
    val LanguageVersion(major, minor) = version

    major match {
      case LanguageMajorVersion.V2 =>
        PLF.ArchivePayload
          .newBuilder()
          .setMinor(minor.toProtoIdentifier)
          .setDamlLf2(new EncodeV2(minor).encodePackage(pkgId, pkg))
          .build()
      case _ =>
        sys.error(s"$version not supported")
    }
  }

  final def encodeArchive(pkg: (PackageId, Package), version: LanguageVersion): PLF.Archive = {

    val payload =
      try {
        data.assertRight(SafeProto.toByteString(encodePayloadOfVersion(pkg, version)))
      } catch {
        case e: Throwable =>
          e.printStackTrace(System.err)
          throw e
      }
    val hash = PackageId.assertFromString(
      MessageDigestPrototype.Sha256.newDigest
        .digest(payload.toByteArray)
        .map("%02x" format _)
        .mkString
    )

    PLF.Archive
      .newBuilder()
      .setHashFunction(PLF.HashFunction.SHA256)
      .setPayload(payload)
      .setHash(hash)
      .build()

  }

  case class EncodeError(message: String) extends RuntimeException

  private[testing] def unexpectedError(): Unit = throw EncodeError("unexpected error")

  private[testing] def expect(b: Boolean): Unit = if (!b) unexpectedError()
}
