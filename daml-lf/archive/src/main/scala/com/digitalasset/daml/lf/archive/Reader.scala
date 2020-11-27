// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.InputStream
import java.security.MessageDigest

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.daml_lf_dev.DamlLf
import com.google.protobuf.CodedInputStream

abstract class Reader[+Pkg] {
  import Reader._

  // This constant is introduced and used
  // to make serialization of nested data
  // possible otherwise complex models failed to deserialize.
  def PROTOBUF_RECURSION_LIMIT: Int = 1000

  def withRecursionLimit(recursionLimit: Int): Reader[Pkg] = new Reader[Pkg] {
    override val PROTOBUF_RECURSION_LIMIT = recursionLimit
    protected[this] override def readArchivePayloadOfVersion(
        hash: PackageId,
        lf: DamlLf.ArchivePayload,
        version: LanguageVersion
    ): Pkg =
      Reader.this.readArchivePayloadOfVersion(hash, lf, version)
  }

  @throws[ParseError]
  final def readArchiveAndVersion(is: InputStream): (Pkg, LanguageMajorVersion) = {
    val cos = damlLfCodedInputStream(is, PROTOBUF_RECURSION_LIMIT)
    readArchiveAndVersion(DamlLf.Archive.parser().parseFrom(cos))
  }

  @throws[ParseError]
  final def decodeArchiveFromInputStream(is: InputStream): Pkg =
    readArchiveAndVersion(is)._1

  @throws[ParseError]
  final def readArchiveAndVersion(lf: DamlLf.Archive): (Pkg, LanguageMajorVersion) = {
    lf.getHashFunction match {
      case DamlLf.HashFunction.SHA256 =>
        val payload = lf.getPayload.toByteArray
        val theirHash = PackageId.fromString(lf.getHash) match {
          case Right(hash) => hash
          case Left(err) => throw ParseError(s"Invalid hash: $err")
        }
        val ourHash =
          PackageId.assertFromString(
            MessageDigest.getInstance("SHA-256").digest(payload).map("%02x" format _).mkString)
        if (ourHash != theirHash) {
          throw ParseError(s"Mismatching hashes! Expected $ourHash but got $theirHash")
        }
        val cos = damlLfCodedInputStreamFromBytes(payload, PROTOBUF_RECURSION_LIMIT)
        readArchivePayloadAndVersion(ourHash, DamlLf.ArchivePayload.parser().parseFrom(cos))
      case DamlLf.HashFunction.UNRECOGNIZED =>
        throw ParseError("Unrecognized hash function")
    }
  }

  @throws[ParseError]
  final def decodeArchive(lf: DamlLf.Archive): Pkg =
    readArchiveAndVersion(lf)._1

  @throws[ParseError]
  final def readArchivePayload(hash: PackageId, lf: DamlLf.ArchivePayload): Pkg =
    readArchivePayloadAndVersion(hash, lf)._1

  @throws[ParseError]
  final def readArchivePayloadAndVersion(
      hash: PackageId,
      lf: DamlLf.ArchivePayload): (Pkg, LanguageMajorVersion) = {
    val majorVersion = readArchiveVersion(lf)
    val minorVersion = lf.getMinor
    val version =
      LanguageVersion(majorVersion, LanguageVersion.Minor fromProtoIdentifier minorVersion)
    if (!(majorVersion supportsMinorVersion minorVersion)) {
      throw ParseError(
        s"LF file $majorVersion.$minorVersion unsupported; maximum supported $majorVersion.x is $majorVersion.${majorVersion.maxSupportedStableMinorVersion.toProtoIdentifier: String}")
    }
    (readArchivePayloadOfVersion(hash, lf, version), majorVersion)
  }

  protected[this] def readArchivePayloadOfVersion(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion): Pkg
}

object Reader extends Reader[(PackageId, DamlLf.ArchivePayload)] {

  final case class ParseError(error: String) extends RuntimeException(error)

  def damlLfCodedInputStreamFromBytes(
      payload: Array[Byte],
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT): CodedInputStream = {
    val cos = com.google.protobuf.CodedInputStream.newInstance(payload)
    cos.setRecursionLimit(recursionLimit)
    cos
  }

  def damlLfCodedInputStream(
      is: InputStream,
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT): CodedInputStream = {
    val cos = com.google.protobuf.CodedInputStream.newInstance(is)
    cos.setRecursionLimit(recursionLimit)
    cos
  }

  @throws[ParseError]
  def readArchiveVersion(lf: DamlLf.ArchivePayload): LanguageMajorVersion = {
    import DamlLf.ArchivePayload.{SumCase => SC}
    import language.{LanguageMajorVersion => LMV}
    lf.getSumCase match {
      case SC.DAML_LF_1 => LMV.V1
      case SC.SUM_NOT_SET => throw ParseError("Unrecognized LF version")
    }
  }

  protected[this] override def readArchivePayloadOfVersion(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion,
  ): (PackageId, DamlLf.ArchivePayload) = (hash, lf)

  // Archive Reader that just checks package hash.
  val HashChecker = new Reader[Unit] {
    override protected[this] def readArchivePayloadOfVersion(
        hash: PackageId,
        lf: DamlLf.ArchivePayload,
        version: LanguageVersion,
    ): Unit = ()
  }

}
