// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.{v30, v30old}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class InitializeSequencerResponse(
    keyId: String,
    publicKey: SigningPublicKey,
    replicated: Boolean,
) {
  def toProtoV30Old: v30old.InitResponse = v30old.InitResponse(
    keyId = keyId,
    publicKey = Some(publicKey.toProtoV30),
    replicated = replicated,
  )
}

object InitializeSequencerResponse {
  def fromProtoV30Old(response: v30old.InitResponse): ParsingResult[InitializeSequencerResponse] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "public_key",
        response.publicKey,
      )
    } yield InitializeSequencerResponse(response.keyId, publicKey, response.replicated)
}

final case class InitializeSequencerResponseX(replicated: Boolean) {
  def toProtoV30: v30.InitializeSequencerResponse =
    v30.InitializeSequencerResponse(replicated)
}

object InitializeSequencerResponseX {
  def fromProtoV30(
      response: v30.InitializeSequencerResponse
  ): ParsingResult[InitializeSequencerResponseX] = {
    val v30.InitializeSequencerResponse(replicated) = response
    Right(InitializeSequencerResponseX(replicated))
  }
}
