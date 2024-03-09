// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.value.ValueCoder.CidEncoder as LfDummyCidEncoder
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}

object GlobalKeySerialization {

  private val NoKeyPackageName = KeyPackageName.assertBuild(None, LfTransactionVersion.V15)

  private def assertRight[T](keyE: Either[String, T]): T =
    keyE
      .fold(
        err => throw new IllegalArgumentException(s"Can't encode contract key: $err"),
        identity,
      )

  def toProtoV0(globalKey: LfVersioned[LfGlobalKey]): Either[String, v0.GlobalKey] = {
    val serializedTemplateId =
      ValueCoder.encodeIdentifier(globalKey.unversioned.templateId).toByteString
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      serializedKey <- ValueCoder
        .encodeVersionedValue(LfDummyCidEncoder, globalKey.version, globalKey.unversioned.key)
        .map(_.toByteString)
        .leftMap(_.errorMessage)
    } yield v0.GlobalKey(serializedTemplateId, serializedKey)
  }

  def assertToProtoV0(key: LfVersioned[LfGlobalKey]): v0.GlobalKey =
    assertRight(toProtoV0(key))

  def fromProtoV0(protoKey: v0.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] =
    for {
      pTemplateId <- ProtoConverter.protoParser(ValueOuterClass.Identifier.parseFrom)(
        protoKey.templateId
      )
      templateId <- ValueCoder
        .decodeIdentifier(pTemplateId)
        .leftMap(err =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.templateId", err.errorMessage)
        )
      deserializedProtoKey <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(
        protoKey.key
      )

      versionedKey <- ValueCoder
        .decodeVersionedValue(ValueCoder.CidDecoder, deserializedProtoKey)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )

      globalKey <- LfGlobalKey
        .build(templateId, versionedKey.unversioned, NoKeyPackageName)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield LfVersioned(versionedKey.version, globalKey)

  def toProtoV1(globalKey: LfVersioned[LfGlobalKey]): Either[String, v1.GlobalKey] = {
    val serializedTemplateId =
      ValueCoder.encodeIdentifier(globalKey.unversioned.templateId).toByteString
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      serializedKey <- ValueCoder
        .encodeVersionedValue(LfDummyCidEncoder, globalKey.version, globalKey.unversioned.key)
        .map(_.toByteString)
        .leftMap(_.errorMessage)
    } yield v1.GlobalKey(
      serializedTemplateId,
      serializedKey,
      globalKey.unversioned.packageName.getOrElse(v1.GlobalKey.defaultInstance.packageName),
    )
  }

  def assertToProtoV1(key: LfVersioned[LfGlobalKey]): v1.GlobalKey =
    assertRight(toProtoV1(key))

  def fromProtoV1(protoKey: v1.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] =
    for {
      pTemplateId <- ProtoConverter.protoParser(ValueOuterClass.Identifier.parseFrom)(
        protoKey.templateId
      )
      templateId <- ValueCoder
        .decodeIdentifier(pTemplateId)
        .leftMap(err =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.templateId", err.errorMessage)
        )
      deserializedProtoKey <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(
        protoKey.key
      )

      versionedKey <- ValueCoder
        .decodeVersionedValue(ValueCoder.CidDecoder, deserializedProtoKey)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )

      packageName <- protoKey.packageName match {
        case "" => Right(None)
        case s =>
          LfPackageName
            .fromString(s)
            .bimap(
              err =>
                ProtoDeserializationError.ValueDeserializationError("GlobalKey.packageName", err),
              pn => Some(pn),
            )
      }

      keyPackageName <- KeyPackageName
        .build(packageName, versionedKey.version)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.keyPackageName", err)
        )

      globalKey <- LfGlobalKey
        .build(templateId, versionedKey.unversioned, keyPackageName)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield LfVersioned(versionedKey.version, globalKey)

}
