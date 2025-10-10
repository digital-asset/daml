// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.{KnownPhysicalSynchronizerId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, FutureUtil}
import com.digitalasset.canton.{SequencerCounter, SynchronizerAlias}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

/** Listens to topology changes and creates a synchronizer connection config in the synchronizer
  * connection config store if the following requirements are satisfied:
  *
  *   - the topology state is frozen by the synchronizer owners, which also includes the physical
  *     synchronizer id of the successor synchronizer
  *   - there is no configuration for the successor physical synchronizer in the synchronizer
  *     connection configuration store
  *   - all sequencers that are configured in the currently active synchronizer connection for the
  *     given synchronizer alias have announced the connection details for connecting to the
  *     sequencer on the successor synchronizer
  */
class SequencerConnectionSuccessorListener(
    alias: SynchronizerAlias,
    topologyClient: SynchronizerTopologyClient,
    configStore: SynchronizerConnectionConfigStore,
    synchronizerHandshake: HandshakeWithPSId,
    automaticallyConnectToUpgradedSynchronizer: Boolean,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with NamedLogging {

  def init()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    checkAndCreateSynchronizerConfig(topologyClient.approximateTimestamp)

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    Monad[FutureUnlessShutdown].whenA(
      transactions.exists(_.mapping.code == Code.SequencerConnectionSuccessor)
    ) {
      FutureUtil.doNotAwait(
        checkAndCreateSynchronizerConfig(effectiveTimestamp.value.immediateSuccessor)
          .onShutdown(()),
        failureMessage = s"error while migrating sequencer connections for $alias",
      )
      FutureUnlessShutdown.unit
    }

  private def checkAndCreateSynchronizerConfig(
      snapshotTs: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val resultOT = for {
      snapshot <- OptionT.liftF(topologyClient.awaitSnapshot(snapshotTs))
      activeConfig <- OptionT.fromOption[FutureUnlessShutdown](
        configStore.get(topologyClient.psid).toOption
      )
      configuredSequencers =
        activeConfig.config.sequencerConnections.aliasToConnection.forgetNE.toSeq.flatMap {
          case (sequencerAlias, connection) =>
            connection.sequencerId.map(_ -> sequencerAlias)
        }.toMap
      configuredSequencerIds = configuredSequencers.keySet

      (synchronizerUpgradeOngoing, _) <- OptionT(snapshot.synchronizerUpgradeOngoing())
      SynchronizerSuccessor(successorPSId, upgradeTime) = synchronizerUpgradeOngoing

      _ = logger.debug(
        s"Checking whether the participant can migrate $alias from ${activeConfig.configuredPSId} to $successorPSId"
      )
      _ = logger.debug(s"Configured sequencer connections: $configuredSequencerIds")

      sequencerSuccessors <- OptionT.liftF(snapshot.sequencerConnectionSuccessors())

      _ = logger.debug(s"Successors are currently known for: $sequencerSuccessors")

      configuredSequencersWithoutSuccessor = configuredSequencerIds
        .diff(sequencerSuccessors.keySet)
      _ = if (configuredSequencersWithoutSuccessor.nonEmpty)
        logger.debug(
          s"Some sequencer have not yet announced their endpoints on the successor synchronizer: $configuredSequencersWithoutSuccessor"
        )
      _ <- OptionT
        .when[FutureUnlessShutdown, Unit](configuredSequencersWithoutSuccessor.isEmpty)(())

      successorConnections <- OptionT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(sequencerSuccessors.flatMap { case (successorSequencerId, successorConfig) =>
          configuredSequencers.get(successorSequencerId).map { sequencerAlias =>
            successorConfig.toGrpcSequencerConnection(sequencerAlias)
          }
        }.toSeq)
      )

      _ = logger.debug(s"New set of sequencer connections for successors: $successorConnections")

      sequencerConnections <- OptionT.fromOption[FutureUnlessShutdown](
        SequencerConnections
          .many(
            connections = successorConnections,
            activeConfig.config.sequencerConnections.sequencerTrustThreshold,
            activeConfig.config.sequencerConnections.sequencerLivenessMargin,
            activeConfig.config.sequencerConnections.submissionRequestAmplification,
            activeConfig.config.sequencerConnections.sequencerConnectionPoolDelays,
          )
          .toOption
      )

      currentSuccessorConfigO =
        configStore.get(alias, KnownPhysicalSynchronizerId(successorPSId)).toOption
      _ <- currentSuccessorConfigO match {
        case None =>
          val updated = activeConfig.config
            .copy(
              synchronizerId = Some(successorPSId),
              sequencerConnections = sequencerConnections,
            )
          configStore
            .put(
              config = updated,
              status = SynchronizerConnectionConfigStore.UpgradingTarget,
              configuredPSId = KnownPhysicalSynchronizerId(successorPSId),
              synchronizerPredecessor =
                Some(SynchronizerPredecessor(topologyClient.psid, upgradeTime)),
            )
            .toOption
        case Some(currentSuccessorConfig) =>
          val updated =
            currentSuccessorConfig.config.copy(sequencerConnections = sequencerConnections)
          configStore.replace(currentSuccessorConfig.configuredPSId, updated).toOption
      }

      _ = if (automaticallyConnectToUpgradedSynchronizer)
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          synchronizerHandshake
            .performHandshake(successorPSId)
            .value
            .map {
              case Left(error) =>
                val isRetryable = error.retryable.isDefined

                // e.g., transient network or pool errors
                if (isRetryable)
                  logger.info(s"Unable to perform handshake with $successorPSId: $error")
                else
                  logger.error(s"Unable to perform handshake with $successorPSId: $error")

              case Right(_: PhysicalSynchronizerId) =>
                logger.info(s"Handshake with $successorPSId was successful")
            },
          level = Level.INFO,
          failureMessage = s"Failed to perform the synchronizer handshake with $successorPSId",
        )
    } yield ()
    resultOT.value.void

  }
}

trait HandshakeWithPSId {
  def performHandshake(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId]
}
