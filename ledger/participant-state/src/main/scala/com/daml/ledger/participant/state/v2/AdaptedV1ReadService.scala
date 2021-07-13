// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v2.AdaptedV1ReadService._
import com.daml.ledger.participant.state.v2.Update.CommandRejected
import com.daml.ledger.participant.state.v2.Update.CommandRejected.RejectionReasonTemplate

class AdaptedV1ReadService(delegate: v1.ReadService) extends ReadService {
  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    delegate
      .getLedgerInitialConditions()
      .map(adaptLedgerInitialConditions)

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    delegate
      .stateUpdates(beginAfter.map(offset => v1.Offset(offset.bytes)))
      .map { case (offset, update) => Offset(offset.bytes) -> adaptUpdate(update) }

  override def currentHealth(): HealthStatus = delegate.currentHealth()
}

private[v2] object AdaptedV1ReadService {
  def adaptLedgerInitialConditions(
      ledgerInitialConditions: v1.LedgerInitialConditions
  ): LedgerInitialConditions =
    LedgerInitialConditions(
      ledgerId = ledgerInitialConditions.ledgerId,
      config = adaptLedgerConfiguration(ledgerInitialConditions.config),
      initialRecordTime = ledgerInitialConditions.initialRecordTime,
    )

  def adaptLedgerConfiguration(config: v1.Configuration): Configuration =
    Configuration(
      generation = config.generation,
      timeModel = adaptTimeModel(config.timeModel),
      maxDeduplicationTime = config.maxDeduplicationTime,
    )

  def adaptTimeModel(timeModel: v1.TimeModel): TimeModel =
    TimeModel(
      timeModel.avgTransactionLatency,
      timeModel.minSkew,
      timeModel.maxSkew,
    ).get

  def adaptUpdate(update: v1.Update): Update = update match {
    case v1.Update.ConfigurationChanged(
          recordTime,
          submissionId,
          participantId,
          newConfiguration,
        ) =>
      Update.ConfigurationChanged(
        recordTime = recordTime,
        submissionId = submissionId,
        participantId = participantId,
        newConfiguration = adaptLedgerConfiguration(newConfiguration),
      )
    case v1.Update.ConfigurationChangeRejected(
          recordTime,
          submissionId,
          participantId,
          proposedConfiguration,
          rejectionReason,
        ) =>
      Update.ConfigurationChangeRejected(
        recordTime = recordTime,
        submissionId = submissionId,
        participantId = participantId,
        proposedConfiguration = adaptLedgerConfiguration(proposedConfiguration),
        rejectionReason = rejectionReason,
      )
    case v1.Update.PartyAddedToParticipant(
          party,
          displayName,
          participantId,
          recordTime,
          submissionId,
        ) =>
      Update.PartyAddedToParticipant(
        party = party,
        displayName = displayName,
        participantId = participantId,
        recordTime = recordTime,
        submissionId = submissionId,
      )
    case v1.Update.PartyAllocationRejected(
          submissionId,
          participantId,
          recordTime,
          rejectionReason,
        ) =>
      Update.PartyAllocationRejected(
        submissionId = submissionId,
        participantId = participantId,
        recordTime = recordTime,
        rejectionReason = rejectionReason,
      )
    case v1.Update.PublicPackageUpload(archives, sourceDescription, recordTime, submissionId) =>
      Update.PublicPackageUpload(
        archives = archives,
        sourceDescription = sourceDescription,
        recordTime = recordTime,
        submissionId = submissionId,
      )
    case v1.Update.PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
      Update.PublicPackageUploadRejected(
        submissionId = submissionId,
        recordTime = recordTime,
        rejectionReason = rejectionReason,
      )
    case v1.Update.TransactionAccepted(
          optSubmitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          recordTime,
          divulgedContracts,
          blindingInfo,
        ) =>
      val optCompletionInfo = optSubmitterInfo.map(createCompletionInfo)
      Update.TransactionAccepted(
        optCompletionInfo = optCompletionInfo,
        transactionMeta = adaptTransactionMeta(transactionMeta),
        transaction = transaction,
        transactionId = transactionId,
        recordTime = recordTime,
        divulgedContracts = divulgedContracts.map(adaptDivulgedContract),
        blindingInfo = blindingInfo,
      )
    case v1.Update.CommandRejected(recordTime, submitterInfo, reason) =>
      Update.CommandRejected(
        recordTime = recordTime,
        completionInfo = createCompletionInfo(submitterInfo),
        reasonTemplate = adaptRejectionReason(reason),
      )
  }

  def adaptTransactionMeta(transactionMeta: v1.TransactionMeta): TransactionMeta =
    TransactionMeta(
      ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
      workflowId = transactionMeta.workflowId,
      submissionTime = transactionMeta.submissionTime,
      submissionSeed = transactionMeta.submissionSeed,
      optUsedPackages = transactionMeta.optUsedPackages,
      optNodeSeeds = transactionMeta.optNodeSeeds,
      optByKeyNodes = transactionMeta.optByKeyNodes,
    )

  private def createCompletionInfo(submitterInfo: v1.SubmitterInfo): CompletionInfo =
    CompletionInfo(
      actAs = submitterInfo.actAs,
      applicationId = submitterInfo.applicationId,
      commandId = submitterInfo.commandId,
      optDeduplicationPeriod = None, // We cannot infer the deduplication period used.
      submissionId = SubmissionId.assertFromString(s"submission-${UUID.randomUUID()}"),
    )

  private def adaptRejectionReason(reason: v1.RejectionReason): RejectionReasonTemplate = {
    val rpcStatus =
      com.google.rpc.status.Status.of(reason.code.value(), reason.description, Seq.empty)
    new CommandRejected.FinalReason(rpcStatus)
  }

  private def adaptDivulgedContract(divulgedContract: v1.DivulgedContract): DivulgedContract =
    DivulgedContract(
      contractId = divulgedContract.contractId,
      contractInst = divulgedContract.contractInst,
    )
}
