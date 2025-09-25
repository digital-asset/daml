// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.AcsCommitmentStore.ReinitializationStatus
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncEphemeralStateFactory,
  SyncPersistentStateLookup,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** Implements the repair commands for ACS commitments. These do not require that the participants
  * disconnects from synchronizers.
  */
final class CommitmentsService(
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    parameters: ParticipantNodeParameters,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    @VisibleForTesting
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private def readCommitmentRepairStatus(
      synchronizerState: SyncPersistentState,
      expectedCompletionTs: CantonTimestamp,
      timeout: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, Option[CantonTimestamp]]] =
    // retry until timeout, namely retry every second for maximum `timeout.duration.getSeconds` times
    retry
      .Backoff(
        logger,
        this,
        // we retry every second
        maxRetries = timeout.duration.getSeconds.intValue,
        1.second,
        1.second,
        "retrieving status of commitment reinitialization",
      )
      .unlessShutdown(
        for {
          ReinitializationStatus(_, reinitCompletedTs) <-
            synchronizerState.acsCommitmentStore.runningCommitments
              .readReinitilizationStatus()
          result =
            if (reinitCompletedTs.contains(expectedCompletionTs))
              Right(reinitCompletedTs)
            else {
              Left(
                s"Commitment reinitialization not completed yet, expected completion timestamp: $expectedCompletionTs, actual: $reinitCompletedTs"
              )
            }
        } yield result,
        NoExceptionRetryPolicy,
      )

  def reinitializeCommitmentsUsingAcs(
      paramSynchronizerIds: Set[SynchronizerId],
      filterCounterParticipants: Seq[ParticipantId],
      filterParties: Seq[PartyId],
      timeoutSeconds: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerId, Either[String, Option[CantonTimestamp]]]] = {
    // TODO(#27370) Wire through filterCounterParticipants and filterParties
    logger.debug(
      "Called reinitializeCommitmentsUsingAcs with parameters",
      Map(
        "synchronizerIds" -> paramSynchronizerIds,
        "filterCounterParticipants" -> filterCounterParticipants,
        "filterParties" -> filterParties,
        "timeoutSeconds" -> timeoutSeconds,
      ),
    )
    val synchronizers = connectedSynchronizersLookup.snapshot.values.filter(synchronizer =>
      paramSynchronizerIds.isEmpty || paramSynchronizerIds.contains(synchronizer.synchronizerId)
    )

    val resultPerSynchronizer = synchronizers.map { synchronizer =>
      val synchronizerId = synchronizer.synchronizerId
      val statusPerSynchronizer =
        for {
          synchronizerIndex <- EitherT
            .right(
              ledgerApiIndexer.value.ledgerApiStore.value
                .cleanSynchronizerIndex(
                  synchronizerId
                )
            )
          reinitRecordTime = SyncEphemeralStateFactory.currentRecordTime(synchronizerIndex)

          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            synchronizer.acsCommitmentProcessor.reinitializeCommitments(reinitRecordTime),
            s"Reinitialization is already scheduled or in progress for ${synchronizer.synchronizerId}.",
          )

          persistentState <- EitherT.fromEither[FutureUnlessShutdown](
            syncPersistentStateLookup.getAll
              .get(synchronizerId)
              .toRight(
                s"Could not find persistent state for ${synchronizer.synchronizerId}"
              )
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            Either.cond(
              !persistentState.isMemory,
              (),
              s"${synchronizer.synchronizerId} is in memory which is not supported by commitment reinitialization. Use db persistence.",
            )
          )
          res <- EitherT(
            readCommitmentRepairStatus(
              persistentState,
              reinitRecordTime,
              timeoutSeconds,
            )
          )
        } yield res
      statusPerSynchronizer.value.map(status => synchronizerId -> status)
    }
    resultPerSynchronizer.toSeq.sequence.map(_.toMap)
  }
}
