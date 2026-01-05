// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.daml.lf.crypto

object GlobalKeySerialization {

  def toProto(globalKey: LfVersioned[LfGlobalKey]): Either[String, v30.GlobalKey] = {
    val templateIdP = ValueCoder.encodeIdentifier(globalKey.unversioned.templateId)
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      keyP <- ValueCoder
        .encodeVersionedValue(globalKey.map(_.key))
        .leftMap(_.errorMessage)
    } yield v30.GlobalKey(
      templateId = templateIdP.toByteString,
      key = globalKey.unversioned.hash.bytes.toByteString.concat(keyP.toByteString),
      globalKey.unversioned.packageName,
    )
  }

  def assertToProto(key: LfVersioned[LfGlobalKey]): v30.GlobalKey =
    toProto(key)
      .valueOr(err => throw new IllegalArgumentException(s"Can't encode contract key: $err"))

  def fromProtoV30(globalKeyP: v30.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] = {
    val v30.GlobalKey(templateIdBytes, keyBytes, packageNameP) = globalKeyP
    val hashBytesLength = crypto.Hash.underlyingHashLength
    if (keyBytes.size < hashBytesLength) {
      Left(
        ProtoDeserializationError.OtherError(
          s"ByteString too short to contain a GlobalKey hash. Expected at least $hashBytesLength bytes, got ${keyBytes.size}"
        )
      )
    } else {
      val hashBs = keyBytes.substring(0, hashBytesLength)
      val valueBs = keyBytes.substring(hashBytesLength)
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

        hash <- crypto.Hash.fromBytes(Bytes.fromByteString(hashBs)).left.map(ProtoDeserializationError.OtherError(_))
        keyP <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(valueBs)
        versionedKey <- ValueCoder
          .decodeVersionedValue(keyP)
          .leftMap(err =>
            ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
          )

        packageName <- Ref.PackageName
          .fromString(packageNameP)
          .leftMap(err =>
            ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err)
          )

        globalKey <- LfGlobalKey
          .build(templateId, packageName, versionedKey.unversioned, hash)
          .leftMap(err =>
            ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
          )

      } yield LfVersioned(versionedKey.version, globalKey)
    }
  }
}
