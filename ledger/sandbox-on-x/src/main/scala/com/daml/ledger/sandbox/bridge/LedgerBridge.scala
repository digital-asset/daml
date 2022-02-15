// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.runner.common.{Config, ParticipantConfig}
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.sandbox.{BridgeConfig, BridgeConfigProvider}
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.{CommittedTransaction, TransactionNodeStatistics}
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.common.primitives.Longs

import java.util.UUID
import scala.concurrent.ExecutionContext

trait LedgerBridge {
  def flow: Flow[Submission, (Offset, Update), NotUsed]
}

object LedgerBridge {
  def owner(
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      indexService: IndexService,
      bridgeMetrics: BridgeMetrics,
      servicesThreadPoolSize: Int,
      timeProvider: TimeProvider,
  )(implicit
      loggingContext: LoggingContext,
      servicesExecutionContext: ExecutionContext,
  ): ResourceOwner[LedgerBridge] =
    if (config.extra.conflictCheckingEnabled)
      buildConfigCheckingLedgerBridge(
        config,
        participantConfig,
        indexService,
        bridgeMetrics,
        servicesThreadPoolSize,
        timeProvider,
      )
    else
      ResourceOwner.forValue(() =>
        new PassThroughLedgerBridge(participantConfig.participantId, timeProvider)
      )

  private def buildConfigCheckingLedgerBridge(
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      indexService: IndexService,
      bridgeMetrics: BridgeMetrics,
      servicesThreadPoolSize: Int,
      timeProvider: TimeProvider,
  )(implicit
      loggingContext: LoggingContext,
      servicesExecutionContext: ExecutionContext,
  ) =
    for {
      initialLedgerEnd <- ResourceOwner.forFuture(() => indexService.currentLedgerEnd())
      initialLedgerConfiguration <- ResourceOwner.forFuture(() =>
        indexService.lookupConfiguration().map(_.map(_._2))
      )
      allocatedPartiesAtInitialization <- ResourceOwner.forFuture(() =>
        indexService.listKnownParties().map(_.map(_.party).toSet)
      )
    } yield ConflictCheckingLedgerBridge(
      participantId = participantConfig.participantId,
      indexService = indexService,
      timeProvider = timeProvider,
      initialLedgerEnd =
        Offset.fromHexString(Ref.HexString.assertFromString(initialLedgerEnd.value)),
      initialAllocatedParties = allocatedPartiesAtInitialization,
      initialLedgerConfiguration = initialLedgerConfiguration,
      bridgeMetrics = bridgeMetrics,
      errorFactories = ErrorFactories(),
      validatePartyAllocation = !config.extra.implicitPartyAllocation,
      servicesThreadPoolSize = servicesThreadPoolSize,
      maxDeduplicationDuration = initialLedgerConfiguration
        .map(_.maxDeduplicationDuration)
        .getOrElse(
          BridgeConfigProvider.initialLedgerConfig(config).configuration.maxDeduplicationDuration
        ),
    )

  private[bridge] def packageUploadSuccess(
      s: Submission.UploadPackages,
      currentTimestamp: Time.Timestamp,
  ): Update.PublicPackageUpload =
    Update.PublicPackageUpload(
      archives = s.archives,
      sourceDescription = s.sourceDescription,
      recordTime = currentTimestamp,
      submissionId = Some(s.submissionId),
    )

  private[bridge] def configChangedSuccess(
      s: Submission.Config,
      participantId: ParticipantId,
      currentTimestamp: Time.Timestamp,
  ): Update.ConfigurationChanged =
    Update.ConfigurationChanged(
      recordTime = currentTimestamp,
      submissionId = s.submissionId,
      participantId = participantId,
      newConfiguration = s.config,
    )

  private[bridge] def partyAllocationSuccess(
      s: Submission.AllocateParty,
      participantId: ParticipantId,
      currentTimestamp: Time.Timestamp,
  ): Update.PartyAddedToParticipant = {
    val party =
      s.hint.getOrElse(Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}"))
    Update.PartyAddedToParticipant(
      party = Ref.Party.assertFromString(party),
      displayName = s.displayName.getOrElse(""),
      participantId = participantId,
      recordTime = currentTimestamp,
      submissionId = Some(s.submissionId),
    )
  }

  private[sandbox] def transactionAccepted(
      transactionSubmission: Submission.Transaction,
      index: Long,
      currentTimestamp: Time.Timestamp,
  ): Update.TransactionAccepted = {
    val submittedTransaction = transactionSubmission.transaction
    val completionInfo = Some(
      transactionSubmission.submitterInfo.toCompletionInfo(
        Some(TransactionNodeStatistics(submittedTransaction))
      )
    )
    Update.TransactionAccepted(
      optCompletionInfo = completionInfo,
      transactionMeta = transactionSubmission.transactionMeta,
      transaction = CommittedTransaction(submittedTransaction),
      transactionId = Ref.TransactionId.assertFromString(index.toString),
      recordTime = currentTimestamp,
      divulgedContracts = Nil,
      blindingInfo = None,
    )
  }

  private[bridge] def fromOffset(offset: Offset): Long = {
    val offsetBytes = offset.toByteArray
    if (offsetBytes.length > 8)
      throw new RuntimeException(s"Byte array too big: ${offsetBytes.length}")
    else
      Longs.fromByteArray(
        Array.fill[Byte](8 - offsetBytes.length)(0) ++ offsetBytes
      )
  }

  private[bridge] def toOffset(index: Long): Offset = Offset.fromByteArray(Longs.toByteArray(index))
}
