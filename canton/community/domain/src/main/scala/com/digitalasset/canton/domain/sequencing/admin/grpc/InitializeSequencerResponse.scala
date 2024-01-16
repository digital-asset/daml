// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class InitializeSequencerResponse(
    keyId: String,
    publicKey: SigningPublicKey,
    replicated: Boolean,
) {
  def toProtoV0: v0.InitResponse = v0.InitResponse(
    keyId = keyId,
    publicKey = Some(publicKey.toProtoV0),
    replicated = replicated,
  )
}

object InitializeSequencerResponse {
  def fromProtoV0(response: v0.InitResponse): ParsingResult[InitializeSequencerResponse] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
        "public_key",
        response.publicKey,
      )
    } yield InitializeSequencerResponse(response.keyId, publicKey, response.replicated)
}

final case class InitializeSequencerResponseX(replicated: Boolean) {
  def toProtoV2: v2.InitializeSequencerResponse =
    v2.InitializeSequencerResponse(replicated)
}

object InitializeSequencerResponseX {
  def fromProtoV2(
      response: v2.InitializeSequencerResponse
  ): ParsingResult[InitializeSequencerResponseX] = {
    val v2.InitializeSequencerResponse(replicated) = response
    Right(InitializeSequencerResponseX(replicated))
  }
}
