// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SequencerCounter, topology}

import scala.concurrent.Future

// TODO(i18695): Clean up this and the trait as polymorphism is not needed here anymore
class ParticipantTopologyTerminateProcessingTicker(
    recordOrderPublisher: RecordOrderPublisher,
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext): Future[Unit] =
    recordOrderPublisher.tick(sc, sequencedTime.value, eventO = None, requestCounterO = None)
}
