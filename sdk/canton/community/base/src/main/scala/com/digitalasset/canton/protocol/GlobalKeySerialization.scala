// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

object GlobalKeySerialization {

  def toProto(globalKey: LfGlobalKey): Either[String, v30.GlobalKey] = {
    val templateIdP =
      ValueCoder.encodeIdentifier(globalKey.templateId).toByteString
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      keyP <- ValueCoder
        .encodeValue(valueVersion = LfTransactionVersion.maxVersion, globalKey.key)
        .leftMap(_.errorMessage)
    } yield v30.GlobalKey(templateId = templateIdP, key = keyP)
  }

  def assertToProto(key: LfGlobalKey): v30.GlobalKey =
    toProto(key)
      .fold(
        err => throw new IllegalArgumentException(s"Can't encode contract key: $err"),
        identity,
      )

  def fromProtoV30(globalKeyP: v30.GlobalKey): ParsingResult[LfGlobalKey] = {
    val v30.GlobalKey(templateIdBytes, keyP) = globalKeyP
    for {
      templateIdP <- ProtoConverter.protoParser(ValueOuterClass.Identifier.parseFrom)(
        templateIdBytes
      )
      templateId <- ValueCoder
        .decodeIdentifier(templateIdP)
        .leftMap(err =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.templateId", err.errorMessage)
        )

      key <- ValueCoder
        .decodeValue(LfTransactionVersion.maxVersion, keyP)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )

      globalKey <- LfGlobalKey
        .build(templateId, key)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield globalKey
  }

}
