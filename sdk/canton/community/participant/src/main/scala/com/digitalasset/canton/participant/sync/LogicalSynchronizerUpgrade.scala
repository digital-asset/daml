// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.SynchronizerSuccessor
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LifeCycleContainer
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgrade.UpgradabilityCheckResult
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.topology.{
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.retry.Backoff
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** Performs the upgrade from one physical synchronizer to its successor.
  *
  * @param executionQueue
  *   Sequential execution queue on which actions must be run. This queue is shared with the
  *   CantonSyncService, which uses it for synchronizer connections. Sharing it ensures that we
  *   cannot connect to the synchronizer while an upgrade action is running and vice versa.
  *
  * @param connectSynchronizer
  *   Function to connect to a synchronizer. Needs to be synchronized using the `executionQueue`.
  * @param disconnectSynchronizer
  *   Function to disconnect to a synchronizer. Needs to be synchronized using the `executionQueue`.
  *
  * The use of the shared execution queue implies the following:
  *   - It introduces the limitation that a single upgrade can be run at a time. This is a
  *     reasonable limitation for now considering the frequency of upgrades.
  *   - Since upgrades should be quick, not allowing reconnects to other synchronizers is reasonable
  *     as well.
  */
final class LogicalSynchronizerUpgrade(
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    syncPersistentStateManager: SyncPersistentStateManager,
    executionQueue: SimpleExecutionQueue,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  // Retry policy for the readiness check and upgrade
  private val retryPolicy: Backoff = Backoff(
    logger = logger,
    hasSynchronizeWithClosing = this,
    maxRetries = Int.MaxValue,
    initialDelay = LogicalSynchronizerUpgrade.RetryInitialDelay,
    maxDelay = LogicalSynchronizerUpgrade.RetryMaxDelay,
    operationName = "lsu",
  )

  /** Run `operation` only if connectivity to `lsid` matches `shouldBeConnected`.
    * @return
    *   - Left if the operation should be retried
    *   - Right if the operation was successful
    *   - Errors are reported as failed future.
    */
  private def enqueueOperation[T](
      lsid: SynchronizerId,
      operation: => FutureUnlessShutdown[Either[String, T]],
      description: String,
      shouldBeConnected: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[String, T]] =
    executionQueue.executeUS(
      {
        val isConnected = connectedSynchronizersLookup.isConnected(lsid)
        val text = if (isConnected) "" else "not "

        if (isConnected == shouldBeConnected) {
          logger.info(s"Scheduling $description")
          operation
        } else {
          logger.info(s"Not scheduling $description because the node is ${text}connected to $lsid")
          FutureUnlessShutdown.pure(
            Left(s"Not scheduling $description because the node is ${text}connected to $lsid")
          )
        }
      },
      description,
    )

  /** Attempt to perform the upgrade from `currentPSId` to `synchronizerSuccessor`.
    *
    * Prerequisites:
    *   - Time on `currentPSId` has reached `synchronizerSuccessor.upgradeTime`
    *   - Successor synchronizer is registered
    *
    * Strategy:
    *   - All failing operations that have a chance to succeed are retried.
    *
    * Note:
    *   - The upgrade involves operations that are retried, so the method can take some time to
    *     complete.
    */
  def upgrade(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val successorPSId = synchronizerSuccessor.psid

    logger.info(s"Upgrade from $currentPSId to $successorPSId")

    performIfNotUpgradedYet(successorPSId)(
      for {
        upgradabilityCheckResult <- EitherT[FutureUnlessShutdown, String, UpgradabilityCheckResult](
          retryPolicy.unlessShutdown(
            upgradabilityCheck(alias, currentPSId, synchronizerSuccessor),
            DbExceptionRetryPolicy,
          )
        )

        _ <- upgradabilityCheckResult match {
          case UpgradabilityCheckResult.ReadyToUpgrade =>
            logger.info(
              s"Upgrade from $currentPSId to $successorPSId is possible, starting internal upgrade"
            )

            EitherT.liftF[FutureUnlessShutdown, String, Unit](
              retryPolicy
                .unlessShutdown(
                  performUpgrade(alias, currentPSId, synchronizerSuccessor).value,
                  DbExceptionRetryPolicy,
                )
                .map(_ => ())
            )

          case UpgradabilityCheckResult.UpgradeDone =>
            logger.info(s"Upgrade from $currentPSId to $successorPSId already done.")
            EitherTUtil.unitUS
        }

        _ <- connectSynchronizer(Traced(alias)).leftMap(_.toString)
      } yield ()
    )
  }

  /** Runs `f` if the upgrade was not done yet.
    */
  private def performIfNotUpgradedYet(
      successorPSId: PhysicalSynchronizerId
  )(
      f: => EitherT[FutureUnlessShutdown, String, Unit]
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    synchronizerConnectionConfigStore.get(successorPSId) match {
      case Right(
            StoredSynchronizerConnectionConfig(_, SynchronizerConnectionConfigStore.Active, _, _)
          ) =>
        EitherT.pure(())
      case _ => f
    }

  /* Check whether the upgrade can be done. A left indicates that the upgrade cannot be done. A
   * right indicates that the upgrade can be attempted or was already done.
   */
  private def upgradabilityCheck(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, UpgradabilityCheckResult]] = {
    val successorPSId = synchronizerSuccessor.psid
    val lsid = currentPSId.logical

    logger.debug("Attempting upgradability check")

    connectSynchronizer(Traced(alias)).value.flatMap {
      case Left(error) =>
        // Left will lead to a retry
        FutureUnlessShutdown.pure(s"Unable to connect to $alias: $error".asLeft)

      case Right(Some(`successorPSId`)) =>
        FutureUnlessShutdown.pure(UpgradabilityCheckResult.UpgradeDone.asRight)

      case Right(_) =>
        enqueueOperation(
          lsid,
          canBeUpgradedTo(currentPSId, synchronizerSuccessor).value,
          description = "lsu-upgradability-check",
          shouldBeConnected = true,
        )
    }
  }

  private def performUpgrade(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val successorPSId = synchronizerSuccessor.psid
    val lsid = currentPSId.logical

    performIfNotUpgradedYet(successorPSId)(
      for {
        /*
          We can discard the result because failure of the disconnect call will prevent the operation to be performed,
          which will lead to a retry.
         */
        _disconnect <- disconnectSynchronizer(Traced(alias)).leftMap(_.toString)

        _ <- EitherT(
          enqueueOperation(
            lsid,
            operation =
              performUpgradeInternal(alias, currentPSId, synchronizerSuccessor).value.flatMap {
                /*
                Because preconditions are checked, a failure of the upgrade is not something that can be automatically
                recovered from (except DB exceptions). Hence, we transform the left into a failed future which will bubble
                up and interrupt the upgrade. Manual intervention will be required.
                 */
                case Left(error) =>
                  val err =
                    s"Unable to upgrade $currentPSId to $successorPSId. Not trying. Cause: $error"
                  logger.error(err)
                  FutureUnlessShutdown.failed(new RuntimeException(err))

                case Right(()) => FutureUnlessShutdown.pure(().asRight[String])
              },
            description = "lsu-upgrade",
            shouldBeConnected = false,
          )
        )
      } yield ()
    )
  }

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisite:
    *   - Time on the current synchronizer has reached the upgrade time.
    *   - [[canBeUpgradedTo]] returns Right([[UpgradabilityCheckResult.ReadyToUpgrade]])
    */
  private def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      // Should have been checked before but this is cheap
      _ <- canBeUpgradedTo(currentPSId, synchronizerSuccessor)

      // Prevent reconnect to the current/old synchronizer
      _ = logger.info(s"Marking synchronizer connection $currentPSId as inactive")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(currentPSId),
          SynchronizerConnectionConfigStore.Inactive,
        )
        .leftMap(err => s"Unable to mark current synchronizer as inactive: $err")

      // Comes last to indicate that the node is ready to connect to the successor
      _ = logger.info(s"Marking synchronizer connection ${synchronizerSuccessor.psid} as active")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(synchronizerSuccessor.psid),
          SynchronizerConnectionConfigStore.Active,
        )
        .leftMap(err => s"Unable to mark successor synchronizer as active: $err")
    } yield ()

  /** Check whether the upgrade can be done. A left indicates that the upgrade cannot be done. A
    * right indicates that the upgrade can be attempted or was already done.
    */
  private def canBeUpgradedTo(
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, UpgradabilityCheckResult] = {
    val newPSId = synchronizerSuccessor.psid
    val upgradeTime = synchronizerSuccessor.upgradeTime

    def isUpgradeDone(): EitherT[FutureUnlessShutdown, String, Boolean] =
      EitherT
        .fromOptionF(
          ledgerApiIndexer.asEval.value.ledgerApiStore.value
            .cleanSynchronizerIndex(currentPSId.logical),
          s"Unable to get synchronizer index for ${currentPSId.logical}",
        )
        .map(_.recordTime > upgradeTime)

    def upgradeCheck(): EitherT[FutureUnlessShutdown, String, Unit] = for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerConnectionConfigStore
          .get(synchronizerSuccessor.psid)
          .leftMap(_ => s"Successor $newPSId is not registered")
      )

      synchronizerIndex <- EitherT.fromOptionF(
        ledgerApiIndexer.asEval.value.ledgerApiStore.value
          .cleanSynchronizerIndex(currentPSId.logical),
        s"Unable to get synchronizer index for ${currentPSId.logical}",
      )

      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          synchronizerIndex.recordTime == upgradeTime,
          s"Synchronizer index should be at $upgradeTime time but found ${synchronizerIndex.recordTime}",
        )

      runningCommitmentWatermark <- EitherT(
        syncPersistentStateManager
          .get(newPSId)
          .toRight(s"Unable to find persistent state for ${newPSId.logical}")
          .traverse(_.acsCommitmentStore.runningCommitments.watermark)
      ).map(_.timestamp)

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        upgradeTime <= runningCommitmentWatermark,
        s"Running commitment watermark ($runningCommitmentWatermark) did not reach the upgrade time ($upgradeTime) yet",
      )

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        upgradeTime >= runningCommitmentWatermark,
        s"Running commitment watermark ($runningCommitmentWatermark) is already past the upgrade time ($upgradeTime). Upgrade is impossible.",
      )

    } yield ()

    isUpgradeDone().flatMap { isDone =>
      if (isDone) EitherT.pure[FutureUnlessShutdown, String](UpgradabilityCheckResult.UpgradeDone)
      else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
    }
  }
}

private object LogicalSynchronizerUpgrade {
  sealed trait UpgradabilityCheckResult extends Product with Serializable
  object UpgradabilityCheckResult {
    final case object ReadyToUpgrade extends UpgradabilityCheckResult
    final case object UpgradeDone extends UpgradabilityCheckResult
  }

  private val RetryInitialDelay: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS)
  private val RetryMaxDelay: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
}
