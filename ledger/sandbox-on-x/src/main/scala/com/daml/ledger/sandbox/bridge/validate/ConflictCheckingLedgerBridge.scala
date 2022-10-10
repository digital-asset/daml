// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.ledger.sandbox.domain._
import com.daml.lf.data.Ref
import com.daml.lf.transaction.{GlobalKey => LfGlobalKey, Transaction => LfTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedGraph._
import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

private[validate] class ConflictCheckingLedgerBridge(
    bridgeMetrics: BridgeMetrics,
    prepareSubmission: PrepareSubmission,
    tagWithLedgerEnd: TagWithLedgerEnd,
    conflictCheckWithCommitted: ConflictCheckWithCommitted,
    sequence: Sequence,
    servicesThreadPoolSize: Int,
    stageBufferSize: Int,
) extends LedgerBridge {

  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
      .buffered(bridgeMetrics.Stages.PrepareSubmission.bufferBefore, stageBufferSize)
      .mapAsyncUnordered(servicesThreadPoolSize)(prepareSubmission)
      .buffered(bridgeMetrics.Stages.TagWithLedgerEnd.bufferBefore, stageBufferSize)
      .mapAsync(parallelism = 1)(tagWithLedgerEnd)
      .buffered(bridgeMetrics.Stages.ConflictCheckWithCommitted.bufferBefore, stageBufferSize)
      .mapAsync(servicesThreadPoolSize)(conflictCheckWithCommitted)
      .buffered(bridgeMetrics.Stages.Sequence.bufferBefore, stageBufferSize)
      .statefulMapConcat(sequence)
}

private[bridge] object ConflictCheckingLedgerBridge {
  private[validate] type Validation[T] = Either[Rejection, T]
  private[validate] type AsyncValidation[T] = Future[Validation[T]]
  private[validate] type KeyInputs = Map[LfGlobalKey, LfTransaction.KeyInput]
  private[validate] type UpdatedKeys = Map[LfGlobalKey, Option[ContractId]]

  // Conflict checking stages
  private[validate] type PrepareSubmission = Submission => AsyncValidation[PreparedSubmission]
  private[validate] type TagWithLedgerEnd =
    Validation[PreparedSubmission] => AsyncValidation[(Offset, PreparedSubmission)]
  private[validate] type ConflictCheckWithCommitted =
    Validation[(Offset, PreparedSubmission)] => AsyncValidation[(Offset, PreparedSubmission)]
  private[validate] type Sequence =
    () => Validation[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)]

  def apply(
      participantId: Ref.ParticipantId,
      indexService: IndexService,
      timeProvider: TimeProvider,
      initialLedgerEnd: Offset,
      initialAllocatedParties: Set[Ref.Party],
      initialLedgerConfiguration: Option[Configuration],
      bridgeMetrics: BridgeMetrics,
      servicesThreadPoolSize: Int,
      maxDeduplicationDuration: Duration,
      stageBufferSize: Int,
  )(implicit
      servicesExecutionContext: ExecutionContext
  ): ConflictCheckingLedgerBridge =
    new ConflictCheckingLedgerBridge(
      bridgeMetrics = bridgeMetrics,
      prepareSubmission = new PrepareSubmissionImpl(bridgeMetrics),
      tagWithLedgerEnd = new TagWithLedgerEndImpl(indexService, bridgeMetrics),
      conflictCheckWithCommitted = new ConflictCheckWithCommittedImpl(indexService, bridgeMetrics),
      sequence = new SequenceImpl(
        participantId = participantId,
        timeProvider = timeProvider,
        initialLedgerEnd = initialLedgerEnd,
        initialAllocatedParties = initialAllocatedParties,
        initialLedgerConfiguration = initialLedgerConfiguration,
        bridgeMetrics = bridgeMetrics,
        maxDeduplicationDuration = maxDeduplicationDuration,
      ),
      servicesThreadPoolSize = servicesThreadPoolSize,
      stageBufferSize = stageBufferSize,
    )

  private[validate] def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) =
    f(new DamlContextualizedErrorLogger(logger, loggingContext, submissionId))
}
