// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.replica

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.ParticipantServices
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}

import scala.concurrent.ExecutionContext

class ParticipantReplicaManager(
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit
    ec: ExecutionContext
) extends ReplicaManager(exitOnFatalFailures, timeouts, loggerFactory, futureSupervisor) {
  private val participantServicesRef: SingleUseCell[ParticipantServices] =
    new SingleUseCell[ParticipantServices]

  def setInitialized(participantServices: ParticipantServices): Unit =
    participantServicesRef
      .putIfAbsent(participantServices)
      .foreach(_ =>
        noTracingLogger.warn(
          s"Participant services already initialized, ignoring new value $participantServices"
        )
      )

  private def connectSynchronizers(
      cantonSyncService: CantonSyncService
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    EitherTUtil.toFutureUnlessShutdown(
      // Allow to reconnect to the synchronizers while the sync service is still considered passive
      // This is required because the sync service is only considered active while the transition has completed, which involves the reconnect.
      cantonSyncService
        .reconnectSynchronizers(
          ignoreFailures = true,
          isTriggeredManually = false,
          mustBeActive = false,
        )
        .map(_ => ())
        .leftMap(err =>
          new IllegalStateException(
            s"Reconnecting synchronizers should ignore failures, but got error: $err"
          )
        )
    )

  protected override def transitionToActive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    participantServicesRef.get match {
      case Some(participantServices) =>
        logger.info("Participant replica is becoming active")

        for {
          _ <- participantServices.persistentStateContainer.initializeNext()
          _ = logger.info("Participant replica is becoming active: PersistentState started")
          _ <- participantServices.mutablePackageMetadataView.refreshState
          _ = logger.info(
            "Participant replica is becoming active: MutablePackageMetadataView refreshed"
          )
          _ <- participantServices.ledgerApiIndexerContainer.initializeNext()
          _ = logger.info("Participant replica is becoming active: Ledger API Indexer started")
          _ <- participantServices.cantonSyncService.refreshCaches()
          // Run participant node state recovery
          _ = logger.info("Participant replica is becoming active: SyncService initialized")
          // Start up the Ledger API server
          _ <- participantServices.ledgerApiServerContainer.initializeNext()
          _ = logger.info("Participant replica is becoming active: Ledger API Server started")
          // Start up the Ledger API-dependent Canton services
          _ = participantServices.startableStoppableLedgerApiDependentServices.start()
          _ = logger.info(
            "Participant replica is becoming active: Ledger API dependent services started"
          )
          // Reconnect to the synchronizers to start processing
          _ <- connectSynchronizers(participantServices.cantonSyncService)
          _ = logger.info("Participant replica is becoming active: Synchronizers reconnected")
          // Start the schedulers, pruning scheduler depends on the Ledger API server
          _ <- FutureUnlessShutdown.outcomeF(participantServices.schedulers.start())
          _ = logger.info("Participant replica is becoming active: Schedulers started")
        } yield ()

      case None =>
        // If the node is not yet initialized, we will perform the creation of participant services during initialization and not here
        logger.info(
          "Participant replica not yet initialized, not performing active transition here"
        )
        FutureUnlessShutdown.unit
    }

  override def transitionToPassive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    participantServicesRef.get match {
      case Some(participantServices) =>
        logger.info("Participant replica is becoming passive")
        // Stop the schedulers
        participantServices.schedulers.stop()
        logger.info("Participant replica is becoming passive: schedulers stopped")
        participantServices.startableStoppableLedgerApiDependentServices.close()
        logger.info(
          "Participant replica is becoming passive: Ledger API dependent services stopped"
        )
        participantServices.ledgerApiServerContainer.closeCurrent()
        logger.info("Participant replica is becoming passive: Ledger API Server stopped")
        for {
          // Explicitly disconnect from synchronizers
          _ <- EitherTUtil
            .toFutureUnlessShutdown(
              participantServices.cantonSyncService
                .disconnectSynchronizers()
                .leftMap(err =>
                  new RuntimeException(s"Failed to disconnect from synchronizers: $err")
                )
            )
          _ = logger.info("Participant replica is becoming passive: Synchronizers disconnected")
          _ = participantServices.ledgerApiIndexerContainer.closeCurrent()
          _ = logger.info("Participant replica is becoming passive: Ledger API Indexer stopped")
          _ = participantServices.persistentStateContainer.closeCurrent()
          _ = logger.info("Participant replica is becoming passive: PersistentState stopped")
        } yield ()

      case None =>
        logger.info(
          "Participant replica is becoming passive (participant services not initialized yet: nothing to do)"
        )
        FutureUnlessShutdown.unit
    }
}
