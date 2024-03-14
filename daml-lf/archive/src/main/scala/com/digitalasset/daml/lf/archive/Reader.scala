// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.crypto.MessageDigestPrototype
import com.daml.daml_lf_dev.{DamlLf,DamlLf2}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

case class ArchivePayload(
    pkgId: PackageId,
    version: LanguageVersion,
    protoPkg: DamlLf2.Package,
                         )

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

  @throws[Error.Parsing]
  private[this] def readArchiveVersion(
      lf: DamlLf.ArchivePayload
  ): Either[Error, LanguageMajorVersion] =
    lf.getSumCase match {
      case DamlLf.ArchivePayload.SumCase.DAML_LF_2 =>
        Right(LanguageMajorVersion.V2)
      case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
        Left(Error.Parsing("Unrecognized or Unsupported LF version"))
    }

  // Validate hash and version of a DamlLf.ArchivePayload
  @throws[Error.Parsing]
  def readArchivePayload(
      hash: PackageId,
      payload: DamlLf.ArchivePayload,
  ): Either[Error, ArchivePayload] =
    for {
      majorVersion <- readArchiveVersion(payload)
      version <- majorVersion.toVersion(payload.getMinor).left.map(Error.Parsing)
    } yield ArchivePayload(hash,  version, payload.getDamlLf2)

}
