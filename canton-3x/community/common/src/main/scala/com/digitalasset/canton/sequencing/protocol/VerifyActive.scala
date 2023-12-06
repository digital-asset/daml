// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class VerifyActiveRequest() {

  def toProtoV0: v0.SequencerConnect.VerifyActive.Request =
    v0.SequencerConnect.VerifyActive.Request()

  /* We allow serializing this message to a ByteArray despite it implementing ProtoNonSerializable because the serialization
   is (and should) only used in the HttpSequencerClient.
  If you need to save this message in a database, please add an UntypedVersionedMessage message as documented in contributing.md  */
  def toByteArrayV0: Array[Byte] = toProtoV0.toByteString.toByteArray
}

sealed trait VerifyActiveResponse
object VerifyActiveResponse {
  final case class Success(isActive: Boolean) extends VerifyActiveResponse
  final case class Failure(reason: String) extends VerifyActiveResponse

  def fromProtoV0(
      responseP: v0.SequencerConnect.VerifyActive.Response
  ): ParsingResult[VerifyActiveResponse] =
    responseP.value match {
      case v0.SequencerConnect.VerifyActive.Response.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("VerifyActive.Response.value"))
      case v0.SequencerConnect.VerifyActive.Response.Value.Success(success) =>
        Right(Success(success.isActive))
      case v0.SequencerConnect.VerifyActive.Response.Value.Failure(failure) =>
        Right(Failure(failure.reason))
    }
}
