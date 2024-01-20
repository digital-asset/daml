// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

trait InitializeMediatorResponse {
  def toProtoV0: v0.InitializeMediatorResponse // make public
  def toEither: Either[String, SigningPublicKey]
}

object InitializeMediatorResponse {
  final case class Success(mediatorKey: SigningPublicKey) extends InitializeMediatorResponse {
    override def toProtoV0: v0.InitializeMediatorResponse =
      v0.InitializeMediatorResponse(
        v0.InitializeMediatorResponse.Value.Success(
          v0.InitializeMediatorResponse.Success(
            Some(mediatorKey.toProtoV30)
          )
        )
      )

    override def toEither: Either[String, SigningPublicKey] = Right(mediatorKey)
  }

  final case class Failure(reason: String) extends InitializeMediatorResponse {
    override def toProtoV0: v0.InitializeMediatorResponse =
      v0.InitializeMediatorResponse(
        v0.InitializeMediatorResponse.Value.Failure(
          v0.InitializeMediatorResponse.Failure(
            reason
          )
        )
      )

    override def toEither: Either[String, SigningPublicKey] = Left(reason)
  }

  def fromProtoV0(
      responseP: v0.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponse] = {
    def success(
        successP: v0.InitializeMediatorResponse.Success
    ): ParsingResult[InitializeMediatorResponse] =
      for {
        mediatorKey <- ProtoConverter.parseRequired(
          SigningPublicKey.fromProtoV30,
          "mediator_key",
          successP.mediatorKey,
        )
      } yield InitializeMediatorResponse.Success(mediatorKey)

    def failure(
        failureP: v0.InitializeMediatorResponse.Failure
    ): ParsingResult[InitializeMediatorResponse] =
      Right(InitializeMediatorResponse.Failure(failureP.reason))

    responseP.value match {
      case v0.InitializeMediatorResponse.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("value"))
      case v0.InitializeMediatorResponse.Value.Success(value) => success(value)
      case v0.InitializeMediatorResponse.Value.Failure(value) => failure(value)
    }
  }
}

final case class InitializeMediatorResponseX() {
  def toProtoV2: v2.InitializeMediatorResponse = v2.InitializeMediatorResponse()
}

object InitializeMediatorResponseX {

  def fromProtoV2(
      responseP: v2.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponseX] = {
    val v2.InitializeMediatorResponse() = responseP
    Right(InitializeMediatorResponseX())
  }
}
