// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.concurrent.Executors

import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(s"In-memory ledger/participant") {

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      testId: String,
      offsetVersion: Byte,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantState] =
    for {
      dispatcher <- dispatcherOwner
      committerExecutionContext <- ResourceOwner
        .forExecutorService(() => Executors.newCachedThreadPool())
        .map(ExecutionContext.fromExecutorService)
      readerWriter <- new InMemoryLedgerReaderWriter.Owner(
        ledgerId = ledgerId,
        participantId = participantId,
        offsetVersion = offsetVersion,
        keySerializationStrategy = StateKeySerializationStrategy.createDefault(),
        metrics = metrics,
        dispatcher = dispatcher,
        state = InMemoryState.empty,
        engine = Engine.DevEngine(),
        committerExecutionContext = committerExecutionContext,
      )
    } yield new KeyValueParticipantState(readerWriter, readerWriter, metrics)
}
