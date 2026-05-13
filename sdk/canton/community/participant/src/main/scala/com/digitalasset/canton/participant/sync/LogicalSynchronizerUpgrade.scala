// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{SynchronizerPredecessor, SynchronizerSuccessor}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LifeCycleContainer
import com.digitalasset.canton.participant.admin.data.ManualLSURequest
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownPSId
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.AutomaticLogicalSynchronizerUpgrade.UpgradabilityCheckResult
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.topology.transaction.{
  SynchronizerUpgradeAnnouncement,
  TopologyMapping,
}
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

/** Performs the upgrade from one physical synchronizer to its successor. The final step is to mark
  * the successor configuration as active.
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
abstract class LogicalSynchronizerUpgrade[Param](
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    executionQueue: SimpleExecutionQueue,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
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

  def kind: String

  // Retry policy for the readiness check and upgrade
  protected val retryPolicy: Backoff = Backoff(
    logger = logger,
    hasSynchronizeWithClosing = this,
    maxRetries = Int.MaxValue,
    initialDelay = LogicalSynchronizerUpgrade.RetryInitialDelay,
    maxDelay = LogicalSynchronizerUpgrade.RetryMaxDelay,
    operationName = "lsu",
  )

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisite:
    *   - Node is disconnected from the synchronizer
    *   - See prerequisites for each implementation
    */
  protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      params: Param,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]

  /** Run `operation` only if connectivity to `lsid` matches `shouldBeConnected`.
    * @return
    *   - Left if the operation should be retried
    *   - Right if the operation was successful
    *   - Errors are reported as failed future.
    */
  protected def enqueueOperation[T](
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

  /** Runs `f` if the upgrade was not done yet.
    */
  protected def performIfNotUpgradedYet(
      successorPSId: PhysicalSynchronizerId
  )(
      f: => EitherT[FutureUnlessShutdown, String, Unit],
      operation: String,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    synchronizerConnectionConfigStore.get(successorPSId) match {
      case Right(
            StoredSynchronizerConnectionConfig(_, SynchronizerConnectionConfigStore.Active, _, _)
          ) =>
        logger.info(s"Not running $operation as the successor is already marked as active")
        EitherT.pure(())
      case _ =>
        logger.info(s"Running $operation")
        f
    }

  /** This method ensures that the node is disconnected from the synchronizer before performing the
    * upgrade.
    */
  protected def performUpgrade(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      param: Param,
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
            operation = performUpgradeInternal(
              alias,
              currentPSId,
              synchronizerSuccessor,
              param,
            ).value
              .flatMap {
                /*
                Because preconditions are checked, a failure of the upgrade is not something that can be automatically
                recovered from (except DB exceptions). Hence, we transform the left into a failed future which will bubble
                up and interrupt the upgrade. Manual intervention will be required.
                 */
                case Left(error) =>
                  val err =
                    s"Unable to upgrade $currentPSId to $successorPSId. Not retrying. Cause: $error"
                  logger.error(err)
                  FutureUnlessShutdown.failed(new RuntimeException(err))

                case Right(()) => FutureUnlessShutdown.pure(().asRight[String])
              },
            description = s"$kind-lsu-upgrade",
            shouldBeConnected = false,
          )
        )
      } yield (),
      operation = s"$kind upgrade from $currentPSId to $successorPSId",
    )
  }
}

/** This class implements automatic LSU. It should be called for participants that are not upgrading
  * too late (after the old synchronizer has been decomissioned).
  */
class AutomaticLogicalSynchronizerUpgrade(
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
    extends LogicalSynchronizerUpgrade[Unit](
      synchronizerConnectionConfigStore,
      executionQueue,
      connectedSynchronizersLookup,
      disconnectSynchronizer,
      timeouts,
      loggerFactory,
    ) {

  override def kind: String = "automatic"

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

    // Ensure upgraded is not attempted if announcement was revoked
    def ensureUpgradeOngoing(): EitherT[FutureUnlessShutdown, String, Unit] = for {
      topologyStore <- EitherT.fromOption[FutureUnlessShutdown](
        syncPersistentStateManager.get(currentPSId).map(_.topologyManager.store),
        "Unable to find topology store",
      )

      announcements <- EitherT
        .liftF(
          topologyStore.findPositiveTransactions(
            asOf = synchronizerSuccessor.upgradeTime,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(TopologyMapping.Code.SynchronizerUpgradeAnnouncement),
            filterUid = None,
            filterNamespace = None,
          )
        )
        .map(_.collectOfMapping[SynchronizerUpgradeAnnouncement])
        .map(_.result.map(_.transaction.mapping))

      _ <- announcements match {
        case Seq() => EitherT.leftT[FutureUnlessShutdown, Unit]("No synchronizer upgrade ongoing")
        case Seq(head) =>
          EitherT.cond[FutureUnlessShutdown](
            head.successor == synchronizerSuccessor,
            (),
            s"Expected synchronizer successor to be $synchronizerSuccessor but found ${head.successor} in topology state",
          )
        case _more =>
          EitherT.liftF[FutureUnlessShutdown, String, Unit](
            FutureUnlessShutdown.failed(
              new IllegalStateException("Found several SynchronizerUpgradeAnnouncement")
            )
          )
      }
    } yield ()

    performIfNotUpgradedYet(successorPSId)(
      for {
        _ <- ensureUpgradeOngoing()

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
                  performUpgrade(alias, currentPSId, synchronizerSuccessor, ()).value,
                  DbExceptionRetryPolicy,
                )
                .map(_ => ())
            )

          case UpgradabilityCheckResult.UpgradeDone =>
            logger.info(s"Upgrade from $currentPSId to $successorPSId already done.")
            EitherTUtil.unitUS
        }

        _ <- connectSynchronizer(Traced(alias)).leftMap(_.toString)
      } yield (),
      operation = s"automatic upgrade from $currentPSId to $successorPSId",
    )
  }

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisite:
    *   - Time on the current synchronizer has reached the upgrade time.
    *   - `canBeUpgradedTo` returns Right(`ReadyToUpgrade`)
    *   - Node is disconnected from the synchronizer
    */
  override protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      params: Unit,
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
        .leftMap(err => s"Unable to mark current synchronizer $currentPSId as inactive: $err")

      // Comes last to indicate that the node is ready to connect to the successor
      _ = logger.info(s"Marking synchronizer connection ${synchronizerSuccessor.psid} as active")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(synchronizerSuccessor.psid),
          SynchronizerConnectionConfigStore.Active,
        )
        .leftMap(err =>
          s"Unable to mark successor synchronizer ${synchronizerSuccessor.psid} as active: $err"
        )
    } yield ()

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

/** This class implements manual LSU. It should be called for participants that are upgrading
  * manually:
  *   - because automatic LSU failed, or
  *   - because the node is upgrading after the old synchronizer has been decomissioned.
  */
class ManualLogicalSynchronizerUpgrade(
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    executionQueue: SimpleExecutionQueue,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends LogicalSynchronizerUpgrade[SynchronizerConnectionConfig](
      synchronizerConnectionConfigStore,
      executionQueue,
      connectedSynchronizersLookup,
      disconnectSynchronizer,
      timeouts,
      loggerFactory,
    ) {

  override def kind: String = "manual"

  def upgrade(
      request: ManualLSURequest
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val currentPSId = request.currentPSId
    val successorPSId = request.successorPSId
    val upgradeTime = request.upgradeTime
    val alias = request.successorConfig.synchronizerAlias
    val synchronizerSuccessor = SynchronizerSuccessor(successorPSId, upgradeTime)

    EitherT.liftF[FutureUnlessShutdown, String, Unit](
      retryPolicy
        .unlessShutdown(
          performUpgrade(
            alias,
            currentPSId,
            synchronizerSuccessor,
            request.successorConfig,
          ).value,
          DbExceptionRetryPolicy,
        )
        .map(_ => ())
    )
  }

  override protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      successorConfig: SynchronizerConnectionConfig,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking synchronizer connection $currentPSId as inactive")
    val successorPSId = synchronizerSuccessor.psid

    for {
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          successorConfig.synchronizerAlias,
          KnownPhysicalSynchronizerId(currentPSId),
          SynchronizerConnectionConfigStore.Inactive,
        )
        .leftMap(_.message)

      _ <- synchronizerConnectionConfigStore.get(successorPSId) match {
        case Left(_: UnknownPSId) =>
          logger.info(s"Storing synchronizer connection config for $successorPSId")
          synchronizerConnectionConfigStore
            .put(
              successorConfig,
              SynchronizerConnectionConfigStore.Active,
              KnownPhysicalSynchronizerId(successorPSId),
              Some(SynchronizerPredecessor(currentPSId, synchronizerSuccessor.upgradeTime)),
            )
            .leftMap(err => s"Unable to store connection config for $successorPSId: $err")

        case Right(foundConfig) =>
          logger.info(s"Marking synchronizer connection $successorPSId as active")
          synchronizerConnectionConfigStore
            .setStatus(
              successorConfig.synchronizerAlias,
              foundConfig.configuredPSId,
              SynchronizerConnectionConfigStore.Active,
            )
            .leftMap(err => s"Unable to mark successor synchronizer $successorPSId as active: $err")
      }
    } yield ()
  }
}

private object LogicalSynchronizerUpgrade {
  private val RetryInitialDelay: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS)
  private val RetryMaxDelay: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
}

private object AutomaticLogicalSynchronizerUpgrade {
  sealed trait UpgradabilityCheckResult extends Product with Serializable
  object UpgradabilityCheckResult {
    final case object ReadyToUpgrade extends UpgradabilityCheckResult
    final case object UpgradeDone extends UpgradabilityCheckResult
  }
}
