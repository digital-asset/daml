// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.crypto.MessageDigestPrototype
import com.daml.daml_lf_dev.{DamlLf, DamlLf2}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageMinorVersion
import com.daml.lf.language.{LanguageVersion, LanguageMajorVersion}
import com.google.protobuf.ByteString

sealed abstract class ArchivePayload {
  def pkgId: PackageId
  def version: LanguageVersion
}

object ArchivePayload {

final case class Lf1(
                      pkgId: PackageId,
                      proto: ByteString,
                      minor: language.LanguageMinorVersion,
                    ) extends ArchivePayload {
  val version = LanguageVersion(LanguageMajorVersion.V1, minor)
}

final case class Lf2(
                      pkgId: PackageId,
                      proto: DamlLf2.Package,
                      minor: language.LanguageMinorVersion,
                    ) extends ArchivePayload {
  val version = LanguageVersion(LanguageMajorVersion.V2, minor)
}
}

object Reader {

  // Validate hash and version of a DamlLf.Archive
  @throws[Error.Parsing]
  def readArchive(lf: DamlLf.Archive): Either[Error, ArchivePayload] = {
    lf.getHashFunction match {
      case DamlLf.HashFunction.SHA256 =>
        for {
          theirHash <- PackageId
            .fromString(lf.getHash)
            .left
            .map(err => Error.Parsing("Invalid hash: " + err))
          ourHash = MessageDigestPrototype.Sha256.newDigest
            .digest(lf.getPayload.toByteArray)
            .map("%02x" format _)
            .mkString
          _ <- Either.cond(
            theirHash == ourHash,
            (),
            Error.Parsing(s"Mismatching hashes! Expected $ourHash but got $theirHash"),
          )
          proto <- ArchivePayloadParser.fromByteString(lf.getPayload)
          payload <- readArchivePayload(theirHash, proto)

        } yield payload
      case DamlLf.HashFunction.UNRECOGNIZED =>
        Left(Error.Parsing("Unrecognized hash function"))
    }
  }

  // Validate hash and version of a DamlLf.ArchivePayload
  @throws[Error.Parsing]
  def readArchivePayload(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
  ): Either[Error, ArchivePayload] =
    lf.getSumCase match {
      case DamlLf.ArchivePayload.SumCase.DAML_LF_1 =>
        Right(ArchivePayload.Lf1(hash, lf.getDamlLf1, LanguageMinorVersion(lf.getMinor)))
      case DamlLf.ArchivePayload.SumCase.DAML_LF_2 =>
        Right(ArchivePayload.Lf2(hash, lf.getDamlLf2, LanguageMinorVersion(lf.getMinor)))
      case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
        Left(Error.Parsing("Unrecognized or Unsupported LF version"))
    }
}
