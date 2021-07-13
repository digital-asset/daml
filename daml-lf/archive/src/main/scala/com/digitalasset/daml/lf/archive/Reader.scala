// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.InputStream
import java.security.MessageDigest
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Bytes
import com.google.protobuf.CodedInputStream

object Reader {

  @throws[Error.Parsing]
  def readArchive(is: InputStream): ArchivePayload = {
    val cos = damlLfCodedInputStream(is, PROTOBUF_RECURSION_LIMIT)
    readArchive(DamlLf.Archive.parser().parseFrom(cos))
  }

  def readArchive(bytes: Bytes): ArchivePayload =
    readArchive(bytes.toInputStream)

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
        readArchivePayload(ourHash, payload.newInput())
      case DamlLf.HashFunction.UNRECOGNIZED =>
        throw Error.Parsing("Unrecognized hash function")
    }
  }

  def readArchivePayload(hash: PackageId, is: InputStream): ArchivePayload = {
    val cos = damlLfCodedInputStream(is, PROTOBUF_RECURSION_LIMIT)
    readArchivePayload(hash, DamlLf.ArchivePayload.parser().parseFrom(cos))
  }

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

  // This constant is introduced and used
  // to make serialization of nested data
  // possible otherwise complex models failed to deserialize.
  val PROTOBUF_RECURSION_LIMIT: Int = 1000

  def damlLfCodedInputStreamFromBytes(
      payload: Array[Byte],
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT,
  ): CodedInputStream = {
    val cos = com.google.protobuf.CodedInputStream.newInstance(payload)
    cos.setRecursionLimit(recursionLimit)
    cos
  }

  def damlLfCodedInputStream(
      is: InputStream,
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT,
  ): CodedInputStream = {
    val cos = com.google.protobuf.CodedInputStream.newInstance(is)
    cos.setRecursionLimit(recursionLimit)
    cos
  }

  @throws[Error.Parsing]
  def readArchiveVersion(lf: DamlLf.ArchivePayload): LanguageMajorVersion = {
    import DamlLf.ArchivePayload.{SumCase => SC}
    import language.{LanguageMajorVersion => LMV}
    lf.getSumCase match {
      case SC.DAML_LF_1 => LMV.V1
      case SC.SUM_NOT_SET => throw Error.Parsing("Unrecognized LF version")
    }
  }
}

case class ArchivePayload(
    pkgId: PackageId,
    proto: DamlLf.ArchivePayload,
    version: LanguageVersion,
)
