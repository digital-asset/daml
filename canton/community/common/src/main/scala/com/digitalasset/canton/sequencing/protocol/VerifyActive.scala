// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class VerifyActiveRequest() {

  def toProtoV30: v30.SequencerConnect.VerifyActiveRequest =
    v30.SequencerConnect.VerifyActiveRequest()

  /* We allow serializing this message to a ByteArray despite it implementing ProtoNonSerializable because the serialization
   is (and should) only used in the HttpSequencerClient.
  If you need to save this message in a database, please add an UntypedVersionedMessage message as documented in contributing.md  */
  def toByteArrayV30: Array[Byte] = toProtoV30.toByteString.toByteArray
}

sealed trait VerifyActiveResponse

object VerifyActiveResponse {

  final case class Success(isActive: Boolean) extends VerifyActiveResponse
  final case class Failure(reason: String) extends VerifyActiveResponse

  def fromProtoV30(
      responseP: v30.SequencerConnect.VerifyActiveResponse
  ): ParsingResult[VerifyActiveResponse] =
    responseP.value match {
      case v30.SequencerConnect.VerifyActiveResponse.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("VerifyActiveResponse.value"))
      case v30.SequencerConnect.VerifyActiveResponse.Value.Success(success) =>
        Right(Success(success.isActive))
      case v30.SequencerConnect.VerifyActiveResponse.Value.Failure(failure) =>
        Right(Failure(failure.reason))
    }
}
