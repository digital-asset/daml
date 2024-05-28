// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.metrics

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.{GlobalKey, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.metrics.Timed
import com.digitalasset.canton.data.{Offset, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import java.util.concurrent.CompletionStage
import scala.concurrent.Future

final class TimedWriteService(delegate: WriteService, metrics: LedgerApiServerMetrics)
    extends WriteService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      optDomainId: Option[DomainId],
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitTransaction,
      metrics.services.write.submitTransactionRunning,
      delegate.submitTransaction(
        submitterInfo,
        optDomainId,
        transactionMeta,
        transaction,
        estimatedInterpretationCost,
        globalKeyMapping,
        processedDisclosedContracts,
      ),
    )

  def submitReassignment(
      submitter: Ref.Party,
      applicationId: Ref.ApplicationId,
      commandId: Ref.CommandId,
      submissionId: Option[Ref.SubmissionId],
      workflowId: Option[Ref.WorkflowId],
      reassignmentCommand: ReassignmentCommand,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitReassignment,
      metrics.services.write.submitReassignmentRunning,
      delegate.submitReassignment(
        submitter,
        applicationId,
        commandId,
        submissionId,
        workflowId,
        reassignmentCommand,
      ),
    )

  override def uploadDar(
      dar: ByteString,
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.services.write.uploadPackages,
      delegate.uploadDar(dar, submissionId),
    )

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.services.write.allocateParty,
      delegate.allocateParty(hint, displayName, submissionId),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    Timed.completionStage(
      metrics.services.write.prune,
      delegate.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
