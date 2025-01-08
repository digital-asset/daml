// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.DynamicSequencingParametersPayload
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.google.protobuf.ByteString

import java.time.Duration

final case class SequencingParameters(pbftViewChangeTimeout: PositiveFiniteDuration)
object SequencingParameters {

  private val DefaultPbftViewChangeTimeout =
    PositiveFiniteDuration.tryCreate(Duration.ofSeconds(1))

  val Default: SequencingParameters = SequencingParameters(DefaultPbftViewChangeTimeout)

  def fromPayload(payload: ByteString): ParsingResult[SequencingParameters] =
    for {
      payload <- ProtoConverter.protoParser(DynamicSequencingParametersPayload.parseFrom)(payload)
      pbftViewChangeTimeout <- PositiveFiniteDuration.fromProtoPrimitiveO("pbftViewChangeTimeout")(
        payload.pbftViewChangeTimeout
      )
    } yield SequencingParameters(pbftViewChangeTimeout)
}
