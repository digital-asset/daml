// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig}
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.sandbox.BridgeConfig
import com.daml.ledger.sandbox.domain.Submission
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
  )(implicit
      loggingContext: LoggingContext,
      // TODO SoX: Consider using a dedicated thread-pool for the ledger bridge
      servicesExecutionContext: ExecutionContext,
  ): ResourceOwner[LedgerBridge] =
    if (config.extra.conflictCheckingEnabled)
      for {
        initialLedgerEnd <- ResourceOwner.forFuture(() => indexService.currentLedgerEnd())
        conflictCheckingLedgerBridge = new ConflictCheckingLedgerBridge(
          participantId = participantConfig.participantId,
          indexService = indexService,
          initialLedgerEnd =
            Offset.fromHexString(Ref.HexString.assertFromString(initialLedgerEnd.value)),
          bridgeMetrics = bridgeMetrics,
          errorFactories = ErrorFactories(
            new ErrorCodesVersionSwitcher(config.enableSelfServiceErrorCodes)
          ),
          servicesThreadPoolSize = servicesThreadPoolSize,
        )
      } yield conflictCheckingLedgerBridge
    else
      ResourceOwner.forValue(() =>
        new PassThroughLedgerBridge(participantId = participantConfig.participantId)
      )

  private[bridge] def fromOffset(offset: Offset): Long = {
    val offsetBytes = offset.toByteArray
    if (offsetBytes.length > 8)
      throw new RuntimeException(s"Byte array too big: ${offsetBytes.length}")
    else
      Longs.fromByteArray(
        Array.fill[Byte](8 - offsetBytes.length)(0) ++ offsetBytes
      )
  }

  def successMapper(submission: Submission, index: Long, participantId: Ref.ParticipantId): Update =
    submission match {
      case s: Submission.AllocateParty =>
        val party = s.hint.getOrElse(UUID.randomUUID().toString)
        Update.PartyAddedToParticipant(
          party = Ref.Party.assertFromString(party),
          displayName = s.displayName.getOrElse(party),
          participantId = participantId,
          recordTime = Time.Timestamp.now(),
          submissionId = Some(s.submissionId),
        )

      case s: Submission.Config =>
        Update.ConfigurationChanged(
          recordTime = Time.Timestamp.now(),
          submissionId = s.submissionId,
          participantId = participantId,
          newConfiguration = s.config,
        )

      case s: Submission.UploadPackages =>
        Update.PublicPackageUpload(
          archives = s.archives,
          sourceDescription = s.sourceDescription,
          recordTime = Time.Timestamp.now(),
          submissionId = Some(s.submissionId),
        )

      case s: Submission.Transaction =>
        Update.TransactionAccepted(
          optCompletionInfo =
            Some(s.submitterInfo.toCompletionInfo(Some(TransactionNodeStatistics(s.transaction)))),
          transactionMeta = s.transactionMeta,
          transaction = s.transaction.asInstanceOf[CommittedTransaction],
          transactionId = Ref.TransactionId.assertFromString(index.toString),
          recordTime = Time.Timestamp.now(),
          divulgedContracts = Nil,
          blindingInfo = None,
        )
    }

  private[bridge] def toOffset(index: Long): Offset = Offset.fromByteArray(Longs.toByteArray(index))
}
