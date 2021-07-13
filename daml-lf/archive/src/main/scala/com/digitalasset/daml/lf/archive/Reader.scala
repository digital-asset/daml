// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

import java.security.MessageDigest

case class ArchivePayload(
    pkgId: PackageId,
    proto: DamlLf.ArchivePayload,
    version: LanguageVersion,
)

object Reader {

  // Validate hash and version of a DamlLf.Archive
  @throws[Error.Parsing]
  def readArchive(lf: DamlLf.Archive): ArchivePayload = {
    lf.getHashFunction match {
      case DamlLf.HashFunction.SHA256 =>
        val payload = lf.getPayload
        val theirHash = PackageId.fromString(lf.getHash) match {
          case Right(hash) => hash
          case Left(err) => throw Error.Parsing(s"Invalid hash: $err")
        }
        val ourHash =
          PackageId.assertFromString(
            MessageDigest
              .getInstance("SHA-256")
              .digest(payload.toByteArray)
              .map("%02x" format _)
              .mkString
          )
        if (ourHash != theirHash) {
          throw Error.Parsing(s"Mismatching hashes! Expected $ourHash but got $theirHash")
        }
        readArchivePayload(ourHash, ArchivePayloadParser.fromByteString(payload))
      case DamlLf.HashFunction.UNRECOGNIZED =>
        throw Error.Parsing("Unrecognized hash function")
    }
  }

  @throws[Error.Parsing]
  private[this] def readArchiveVersion(lf: DamlLf.ArchivePayload): LanguageMajorVersion =
    lf.getSumCase match {
      case DamlLf.ArchivePayload.SumCase.DAML_LF_1 => LanguageMajorVersion.V1
      case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
        throw Error.Parsing("Unrecognized LF version")
    }

  // Validate hash and version of a DamlLf.ArchivePayload
  @throws[Error.Parsing]
  def readArchivePayload(hash: PackageId, lf: DamlLf.ArchivePayload): ArchivePayload = {
    val majorVersion = readArchiveVersion(lf)
    val minorVersion = lf.getMinor
    val version =
      LanguageVersion(majorVersion, LanguageVersion.Minor(minorVersion))
    if (!(majorVersion supportsMinorVersion minorVersion)) {
      val supportedVersions =
        majorVersion.acceptedVersions.map(v => s"$majorVersion.${v.identifier}")
      throw Error.Parsing(
        s"LF $majorVersion.$minorVersion unsupported. Supported LF versions are ${supportedVersions
          .mkString(",")}"
      )
    }
    ArchivePayload(hash, lf, version)
  }

}
