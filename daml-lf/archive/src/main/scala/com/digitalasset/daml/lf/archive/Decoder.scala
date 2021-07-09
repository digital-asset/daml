// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.{DamlLf, DamlLf1}
import com.daml.lf.data.Bytes
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.daml.lf.language.LanguageMajorVersion.V1
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.google.protobuf.{ByteString, CodedInputStream}

import java.security.MessageDigest

case class ArchivePayload(
    pkgId: PackageId,
    proto: DamlLf.ArchivePayload,
    version: LanguageVersion,
)

final class Decoder[X] private (val fromCodedInputStream: CodedInputStream => X) {

  // close the stream unconditionally
  def fromInputStream(is: java.io.InputStream): X =
    try fromCodedInputStream(CodedInputStream.newInstance(is))
    finally is.close()

  def fromByteArray(bytes: Array[Byte]): X =
    fromCodedInputStream(CodedInputStream.newInstance(bytes))

  def fromByteString(bytes: ByteString): X =
    fromCodedInputStream(CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer()))

  def fromBytes(bytes: Bytes): X =
    fromByteString(bytes.toByteString)

  def fromFile(file: java.nio.file.Path): X =
    fromInputStream(java.nio.file.Files.newInputStream(file))

  private def andThen[Y](f: X => Y): Decoder[Y] =
    new Decoder(x => f(fromCodedInputStream(x)))

}

object Decoder {

  // This constant is introduced and used
  // to make serialization of nested data
  // possible otherwise complex models failed to deserialize.
  private[archive] val PROTOBUF_RECURSION_LIMIT: Int = 1000

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

  @throws[Error.Parsing]
  private[this] def readArchiveVersion(lf: DamlLf.ArchivePayload): LanguageMajorVersion =
    lf.getSumCase match {
      case DamlLf.ArchivePayload.SumCase.DAML_LF_1 => LanguageMajorVersion.V1
      case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
        throw Error.Parsing("Unrecognized LF version")
    }

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

  // decode am ArchivePayload
  def decodeArchivePayload(
      payload: ArchivePayload,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    payload.version match {
      case LanguageVersion(V1, minor) if V1.supportedMinorVersions.contains(minor) =>
        payload.pkgId ->
          new DecodeV1(minor).decodePackage(
            payload.pkgId,
            payload.proto.getDamlLf1,
            onlySerializableDataDefs,
          )
      case v => throw Error.Parsing(s"$v unsupported")
    }

  def decodeArchivePayloads(
      payloads: Iterable[ArchivePayload],
      onlySerializableDataDefs: Boolean = false,
  ): Map[PackageId, Ast.Package] =
    payloads.iterator.map(decodeArchivePayload(_, onlySerializableDataDefs)).toMap

  // decode an Archive
  def decodeArchive(
      archive: DamlLf.Archive,
      onlySerializableDataDefs: Boolean = false,
  ): (PackageId, Ast.Package) =
    decodeArchivePayload(readArchive(archive), onlySerializableDataDefs)

  def decodeArchives(
      archives: Iterable[DamlLf.Archive],
      onlySerializableDataDefs: Boolean = false,
  ): Map[PackageId, Ast.Package] =
    archives.iterator.map(a => decodeArchivePayload(readArchive(a), onlySerializableDataDefs)).toMap

  // just set the recursion limit
  private val base: Decoder[CodedInputStream] = new Decoder[CodedInputStream]({ cos =>
    cos.setRecursionLimit(PROTOBUF_RECURSION_LIMIT)
    cos
  })

  val ArchiveParser: Decoder[DamlLf.Archive] =
    base.andThen(DamlLf.Archive.parseFrom)
  val ArchiveReader: Decoder[ArchivePayload] =
    ArchiveParser.andThen(readArchive)
  val ArchiveDecoder: Decoder[(PackageId, Ast.Package)] =
    ArchiveReader.andThen(decodeArchivePayload(_))

  val ArchivePayloadParser: Decoder[DamlLf.ArchivePayload] =
    base.andThen(DamlLf.ArchivePayload.parseFrom)
  def archivePayloadDecoder(hash: PackageId): Decoder[Ast.Package] =
    ArchivePayloadParser
      .andThen(readArchivePayload(hash, _))
      .andThen(decodeArchivePayload(_)._2)

  private[lf] def moduleDecoder(ver: LanguageVersion, pkgId: PackageId): Decoder[Ast.Module] =
    base
      .andThen(DamlLf1.Package.parseFrom)
      .andThen(new DecodeV1(ver.minor).decodeScenarioModule(pkgId, _))

}
