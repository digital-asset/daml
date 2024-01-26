// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.{v30, v30old}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

trait InitializeMediatorResponse {
  def toProtoV30old: v30old.InitializeMediatorResponse

  def toEither: Either[String, SigningPublicKey]
}

object InitializeMediatorResponse {
  final case class Success(mediatorKey: SigningPublicKey) extends InitializeMediatorResponse {
    override def toProtoV30old: v30old.InitializeMediatorResponse =
      v30old.InitializeMediatorResponse(
        v30old.InitializeMediatorResponse.Value.Success(
          v30old.InitializeMediatorResponse.Success(
            Some(mediatorKey.toProtoV30)
          )
        )
      )

    override def toEither: Either[String, SigningPublicKey] = Right(mediatorKey)
  }

  final case class Failure(reason: String) extends InitializeMediatorResponse {
    override def toProtoV30old: v30old.InitializeMediatorResponse =
      v30old.InitializeMediatorResponse(
        v30old.InitializeMediatorResponse.Value.Failure(
          v30old.InitializeMediatorResponse.Failure(
            reason
          )
        )
      )

    override def toEither: Either[String, SigningPublicKey] = Left(reason)
  }

  def fromProtoV30old(
      responseP: v30old.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponse] = {
    def success(
        successP: v30old.InitializeMediatorResponse.Success
    ): ParsingResult[InitializeMediatorResponse] =
      for {
        mediatorKey <- ProtoConverter.parseRequired(
          SigningPublicKey.fromProtoV30,
          "mediator_key",
          successP.mediatorKey,
        )
      } yield InitializeMediatorResponse.Success(mediatorKey)

    def failure(
        failureP: v30old.InitializeMediatorResponse.Failure
    ): ParsingResult[InitializeMediatorResponse] =
      Right(InitializeMediatorResponse.Failure(failureP.reason))

    responseP.value match {
      case v30old.InitializeMediatorResponse.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("value"))
      case v30old.InitializeMediatorResponse.Value.Success(value) => success(value)
      case v30old.InitializeMediatorResponse.Value.Failure(value) => failure(value)
    }
  }
}

final case class InitializeMediatorResponseX() {
  def toProtoV30: v30.InitializeMediatorResponse = v30.InitializeMediatorResponse()
}

object InitializeMediatorResponseX {

  def fromProtoV30(
      responseP: v30.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponseX] = {
    val v30.InitializeMediatorResponse() = responseP
    Right(InitializeMediatorResponseX())
  }
}
