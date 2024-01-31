// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

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
