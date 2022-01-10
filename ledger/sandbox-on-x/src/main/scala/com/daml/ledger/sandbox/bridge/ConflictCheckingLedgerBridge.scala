// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.sandbox.bridge.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.domain._
import com.daml.lf.data.Ref
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events._

import scala.concurrent.{ExecutionContext, Future}

private[bridge] class ConflictCheckingLedgerBridge(
    // Precomputes the transaction effects for transaction submissions.
    // For other update types, this stage is a no-op.
    prepareSubmission: PrepareSubmission,
    // Tags the prepared submission with the current ledger end as available on the Ledger API.
    tagWithLedgerEnd: TagWithLedgerEnd,
    // Conflict checking for incoming submissions against the ledger state
    // as it is visible on the Ledger API.
    conflictCheckWithCommitted: ConflictCheckWithCommitted,
    // Conflict checking with the in-flight commands,
    // assigns offsets and converts the accepted/rejected commands to updates.
    sequence: Sequence,
    servicesThreadPoolSize: Int,
) extends LedgerBridge {
  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
      .mapAsyncUnordered(servicesThreadPoolSize)(prepareSubmission)
      .mapAsync(parallelism = 1)(tagWithLedgerEnd)
      .mapAsync(servicesThreadPoolSize)(conflictCheckWithCommitted)
      .statefulMapConcat(sequence)
}

private[bridge] object ConflictCheckingLedgerBridge {
  private[bridge] type Validation[T] = Either[Rejection, T]
  private[bridge] type AsyncValidation[T] = Future[Validation[T]]
  private[bridge] type KeyInputs = Map[Key, LfTransaction.KeyInput]

  // Conflict checking stages
  private[bridge] type PrepareSubmission = Submission => AsyncValidation[PreparedSubmission]
  private[bridge] type TagWithLedgerEnd =
    Validation[PreparedSubmission] => AsyncValidation[(Offset, PreparedSubmission)]
  private[bridge] type ConflictCheckWithCommitted =
    Validation[(Offset, PreparedSubmission)] => AsyncValidation[(Offset, PreparedSubmission)]
  private[bridge] type Sequence =
    () => Validation[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)]

  def apply(
      participantId: Ref.ParticipantId,
      indexService: IndexService,
      timeProvider: TimeProvider,
      initialLedgerEnd: Offset,
      initialAllocatedParties: Set[Ref.Party],
      initialLedgerConfiguration: Option[Configuration],
      bridgeMetrics: BridgeMetrics,
      errorFactories: ErrorFactories,
      validatePartyAllocation: Boolean,
      servicesThreadPoolSize: Int,
  )(implicit
      servicesExecutionContext: ExecutionContext
  ): ConflictCheckingLedgerBridge = {
    val prepareSubmission = new PrepareSubmissionImpl(bridgeMetrics)
    val tagWithLedgerEnd = new TagWithLedgerEndImpl(indexService, bridgeMetrics)
    val conflictCheckWithCommitted =
      new ConflictCheckWithCommittedImpl(indexService, bridgeMetrics, errorFactories)
    val sequence = new SequenceImpl(
      participantId = participantId,
      timeProvider = timeProvider,
      initialLedgerEnd = initialLedgerEnd,
      initialAllocatedParties = initialAllocatedParties,
      initialLedgerConfiguration = initialLedgerConfiguration,
      validatePartyAllocation = validatePartyAllocation,
      bridgeMetrics = bridgeMetrics,
      errorFactories = errorFactories,
    )

    new ConflictCheckingLedgerBridge(
      prepareSubmission = prepareSubmission,
      tagWithLedgerEnd = tagWithLedgerEnd,
      conflictCheckWithCommitted = conflictCheckWithCommitted,
      sequence = sequence,
      servicesThreadPoolSize = servicesThreadPoolSize,
    )
  }

  private[bridge] def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) =
    f(new DamlContextualizedErrorLogger(logger, loggingContext, submissionId))
}
