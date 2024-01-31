// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.transaction.Util
import com.daml.lf.value.ValueCoder.CidEncoder as LfDummyCidEncoder
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}

object GlobalKeySerialization {

  def toProto(globalKey: LfVersioned[LfGlobalKey]): Either[String, v30.GlobalKey] = {
    val serializedTemplateId =
      ValueCoder.encodeIdentifier(globalKey.unversioned.templateId).toByteString
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      serializedKey <- ValueCoder
        .encodeVersionedValue(LfDummyCidEncoder, globalKey.version, globalKey.unversioned.key)
        .map(_.toByteString)
        .leftMap(_.errorMessage)
    } yield v30.GlobalKey(serializedTemplateId, serializedKey)
  }

  def assertToProto(key: LfVersioned[LfGlobalKey]): v30.GlobalKey =
    toProto(key)
      .fold(
        err => throw new IllegalArgumentException(s"Can't encode contract key: $err"),
        identity,
      )

  def fromProtoV30(protoKey: v30.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] =
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
        .build(templateId, versionedKey.unversioned, Util.sharedKey(versionedKey.version))
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield LfVersioned(versionedKey.version, globalKey)

}
