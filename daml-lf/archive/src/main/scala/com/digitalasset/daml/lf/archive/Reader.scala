// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import java.io.InputStream
import java.security.MessageDigest

import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml_lf.DamlLf
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
        hash: SimpleString,
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

  @deprecated("use decodeArchiveFromInputStream instead", since = "13.8.1")
  @throws[ParseError]
  final def readArchive(is: InputStream): Pkg = decodeArchiveFromInputStream(is)

  @throws[ParseError]
  final def decodeArchiveFromInputStream(is: InputStream): Pkg =
    readArchiveAndVersion(is)._1

  @throws[ParseError]
  final def readArchiveAndVersion(lf: DamlLf.Archive): (Pkg, LanguageMajorVersion) = {
    lf.getHashFunction match {
      case DamlLf.HashFunction.SHA256 =>
        val payload = lf.getPayload.toByteArray()
        val theirHash = SimpleString.fromString(lf.getHash) match {
          case Right(hash) => hash
          case Left(err) => throw ParseError(s"Invalid hash: $err")
        }
        val ourHash =
          SimpleString.assertFromString(
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

  @deprecated("use decodeArchive instead", since = "13.8.1")
  @throws[ParseError]
  def readArchive(lf: DamlLf.Archive): Pkg = decodeArchive(lf)

  @throws[ParseError]
  final def decodeArchive(lf: DamlLf.Archive): Pkg =
    readArchiveAndVersion(lf)._1

  @throws[ParseError]
  final def readArchivePayload(hash: SimpleString, lf: DamlLf.ArchivePayload): Pkg =
    readArchivePayloadAndVersion(hash, lf)._1

  @throws[ParseError]
  final def readArchivePayloadAndVersion(
      hash: SimpleString,
      lf: DamlLf.ArchivePayload): (Pkg, LanguageMajorVersion) = {
    val majorVersion = readArchiveVersion(lf)
    // for DAML-LF v1, we translate "no version" to minor version 0,
    // since we introduced minor versions once DAML-LF v1 was already
    // out, and we want to be able to parse packages that were compiled
    // before minor versions were a thing. DO NOT replicate this code
    // bejond major version 1!
    val minorVersion = (majorVersion, lf.getMinor) match {
      case (LanguageMajorVersion.V1, "") => "0"
      case (_, minor) => minor
    }
    val version = LanguageVersion(majorVersion, minorVersion)
    if (!(majorVersion supportsMinorVersion minorVersion)) {
      throw ParseError(
        s"LF file $majorVersion.$minorVersion unsupported; maximum supported $majorVersion.x is $majorVersion.${majorVersion.maxSupportedMinorVersion}")
    }
    (readArchivePayloadOfVersion(hash, lf, version), majorVersion)
  }

  protected[this] def readArchivePayloadOfVersion(
      hash: SimpleString,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion): Pkg
}

object Reader extends Reader[(SimpleString, DamlLf.ArchivePayload)] {
  final case class ParseError(error: String) extends RuntimeException(error)

  @deprecated("use lf.archive.LanguageMajorVersion instead", since = "47.0.0")
  type DamlLfMajorVersion = LanguageMajorVersion
  @deprecated("use lf.archive.LanguageMajorVersion.V0 instead", since = "47.0.0")
  val DamlLfVersion0 = LanguageMajorVersion.V0
  @deprecated("use lf.archive.LanguageMajorVersion.V1 instead", since = "47.0.0")
  val DamlLfVersion1 = LanguageMajorVersion.V1
  @deprecated("use lf.archive.LanguageMajorVersion.VDev instead", since = "47.0.0")
  lazy val DamlLfVersionDev = LanguageMajorVersion.VDev

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
    import archive.{LanguageMajorVersion => LMV}
    lf.getSumCase match {
      case SC.DAML_LF_0 => LMV.V0
      case SC.DAML_LF_1 => LMV.V1
      case SC.DAML_LF_DEV => LMV.VDev
      case SC.SUM_NOT_SET => throw ParseError("Unrecognized LF version")
    }
  }

  protected[this] override def readArchivePayloadOfVersion(
      hash: SimpleString,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion,
  ): (SimpleString, DamlLf.ArchivePayload) = (hash, lf)
}
