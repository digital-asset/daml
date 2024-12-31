// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SequencerCounter, topology}

import scala.concurrent.ExecutionContext

class ParticipantTopologyTerminateProcessingTicker(
    override protected val loggerFactory: NamedLoggerFactory
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit
}
