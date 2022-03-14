// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.crypto.MessageDigestPrototype
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

case class ArchivePayload(
    pkgId: PackageId,
    proto: DamlLf.ArchivePayload,
    version: LanguageVersion,
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
      case DamlLf.ArchivePayload.SumCase.DAML_LF_1 =>
        Right(LanguageMajorVersion.V1)
      case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
        Left(Error.Parsing("Unrecognized LF version"))
    }

  // Validate hash and version of a DamlLf.ArchivePayload
  @throws[Error.Parsing]
  def readArchivePayload(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
  ): Either[Error, ArchivePayload] =
    for {
      majorVersion <- readArchiveVersion(lf)
      version <- majorVersion.toVersion(lf.getMinor).left.map(Error.Parsing)
    } yield ArchivePayload(hash, lf, version)

}
