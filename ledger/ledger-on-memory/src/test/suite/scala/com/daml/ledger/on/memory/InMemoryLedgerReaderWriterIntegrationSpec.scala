// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.concurrent.Executors

import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  KeyValueParticipantStateWriter,
}
import com.daml.ledger.participant.state.kvutils.{
  KVOffsetBuilder,
  ParticipantStateIntegrationSpecBase,
}
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext
import scala.util.Random

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(s"In-memory ledger/participant") {

  override val isPersistent: Boolean = false

  // Select a random offset version to ensure that this works with any possible value.
  override val offsetVersion: Byte = {
    val bytes = Array[Byte](Byte.MinValue)
    while (bytes.head < 0) {
      Random.nextBytes(bytes)
    }
    bytes.head
  }

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
      state = InMemoryState.empty
      offsetBuilder = new KVOffsetBuilder(version = offsetVersion)
      writer <- new InMemoryLedgerWriter.Owner(
        participantId = participantId,
        keySerializationStrategy = StateKeySerializationStrategy.createDefault(),
        metrics = metrics,
        dispatcher = dispatcher,
        state = state,
        engine = Engine.DevEngine(),
        committerExecutionContext = committerExecutionContext,
        offsetBuilder = offsetBuilder,
      )
      reader = new InMemoryLedgerReader(ledgerId, dispatcher, offsetBuilder, state, metrics)
    } yield ParticipantStateIntegrationSpecBase.participantStateFrom(
      KeyValueParticipantStateReader(
        reader = reader,
        metrics = metrics,
      ),
      new KeyValueParticipantStateWriter(
        writer = writer,
        metrics = metrics,
      ),
    )
}
