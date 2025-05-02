// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.ProtoDeserializationError.UnknownDriverMetadataVersion
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.v1
import com.digitalasset.daml.lf.data.Bytes as LfBytes
import com.google.protobuf.ByteString

final case class DriverContractMetadata(salt: Salt) extends PrettyPrinting {

  override protected def pretty: Pretty[DriverContractMetadata] = prettyOfClass(
    param("contract salt", _.salt.forHashing)
  )

  def toProtoV30: v30.DriverContractMetadata =
    v30.DriverContractMetadata(Some(salt.toProtoV30))

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def toLfBytes(contractIdVersion: CantonContractIdVersion): LfBytes =
    contractIdVersion match {
      case AuthenticatedContractIdVersionV10 | AuthenticatedContractIdVersionV11 =>
        LfBytes.fromByteArray(
          v1.UntypedVersionedMessage(
            v1.UntypedVersionedMessage.Wrapper.Data(toProtoV30.toByteString),
            30,
          ).toByteArray
        )
    }
}

object DriverContractMetadata {

  def fromLfBytes(bytes: Array[Byte]): ParsingResult[DriverContractMetadata] = for {

    proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- proto.version match {
      case 30 =>
        for {
          data <- ProtoConverter.protoParser(v30.DriverContractMetadata.parseFrom)(
            proto.wrapper.data.getOrElse(ByteString.EMPTY)
          )
          result <- fromProtoV30(data)
        } yield result
      case other => Left(UnknownDriverMetadataVersion(other))
    }
  } yield valueClass

  def fromProtoV30(
      driverContractMetadataP: v30.DriverContractMetadata
  ): ParsingResult[DriverContractMetadata] = {
    val v30.DriverContractMetadata(saltP) = driverContractMetadataP

    ProtoConverter
      .required("salt", saltP)
      .flatMap(Salt.fromProtoV30)
      .map(DriverContractMetadata(_))
  }
}
