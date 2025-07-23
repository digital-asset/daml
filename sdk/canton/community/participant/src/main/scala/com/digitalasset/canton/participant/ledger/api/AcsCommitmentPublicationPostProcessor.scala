// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.ledger.participant.state.Update.{
  AcsChangeSequencedUpdate,
  EmptyAcsPublicationRequired,
  LogicalSynchronizerUpgradeTimeReached,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChangeFactory,
  SynchronizerIndex,
  Update,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookupContainer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

class AcsCommitmentPublicationPostProcessor(
    connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with (Update => Unit) {

  def apply(update: Update): Unit = {
    def publishAcsCommitment(
        synchronizerId: SynchronizerId,
        synchronizerIndex: SynchronizerIndex,
        acsChangeFactoryO: Option[AcsChangeFactory],
    ): Unit =
      connectedSynchronizersLookupContainer
        // not publishing if not connected to synchronizer: it means subsequent crash recovery will establish consistency again
        .get(synchronizerId)
        // not publishing anything if the AcsCommitmentProcessor initialization succeeded with AbortedDueToShutdown or failed
        .foreach(
          _.acsCommitmentProcessor.publish(
            RecordTime.fromSynchronizerIndex(synchronizerIndex),
            acsChangeFactoryO,
          )(
            // The trace context is deliberately generated here instead of continuing the one for the Update
            // to unlink the asynchronous acs commitment processing from message processing trace.
            TraceContext.createNew("publish_acs_commitment")
          )
        )

    update match {
      case updateWithAcsChangeFactory: AcsChangeSequencedUpdate =>
        val (synchronizerId, synchronizerIndex) = updateWithAcsChangeFactory.synchronizerIndex
        publishAcsCommitment(
          synchronizerId,
          synchronizerIndex,
          Some(updateWithAcsChangeFactory.acsChangeFactory),
        )

      case emptyAcsPublicationRequired: EmptyAcsPublicationRequired =>
        val (synchronizerId, synchronizerIndex) = emptyAcsPublicationRequired.synchronizerIndex
        publishAcsCommitment(
          synchronizerId,
          synchronizerIndex,
          acsChangeFactoryO = None,
        )

      case upgradeTimeReached: LogicalSynchronizerUpgradeTimeReached =>
        val (synchronizerId, synchronizerIndex) = upgradeTimeReached.synchronizerIndex

        connectedSynchronizersLookupContainer
          // not publishing if not connected to synchronizer: it means subsequent crash recovery will establish consistency again
          .get(synchronizerId)
          // not publishing anything if the AcsCommitmentProcessor initialization succeeded with AbortedDueToShutdown or failed
          .foreach(
            _.acsCommitmentProcessor.publishForUpgradeTime(
              synchronizerIndex.recordTime
            )(
              // The trace context is deliberately generated here instead of continuing the one for the Update
              // to unlink the asynchronous acs commitment processing from message processing trace.
              TraceContext.createNew("publish_acs_commitment_upgrade_time")
            )
          )

      // not publishing otherwise
      case _ => ()
    }
  }

}
