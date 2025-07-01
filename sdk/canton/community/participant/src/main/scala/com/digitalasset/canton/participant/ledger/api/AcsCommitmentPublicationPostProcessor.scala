// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.ledger.participant.state.Update.EmptyAcsPublicationRequired
import com.digitalasset.canton.ledger.participant.state.{
  CommitSetRepairUpdate,
  CommitSetSequencedUpdate,
  CommitSetUpdate,
  SynchronizerIndex,
  Update,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookupContainer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

class AcsCommitmentPublicationPostProcessor(
    connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with (Update => Unit) {

  def apply(update: Update): Unit = {
    implicit val tc: TraceContext = update.traceContext
    def publishAcsCommitment(
        synchronizerId: SynchronizerId,
        synchronizerIndex: SynchronizerIndex,
        commitSetO: Option[CommitSet],
    ): Unit =
      connectedSynchronizersLookupContainer
        // not publishing if not connected to synchronizer: it means subsequent crash recovery will establish consistency again
        .get(synchronizerId)
        // not publishing anything if the AcsCommitmentProcessor initialization succeeded with AbortedDueToShutdown or failed
        .foreach(
          _.acsCommitmentProcessor.publish(
            RecordTime.fromSynchronizerIndex(synchronizerIndex),
            commitSetO,
          )(
            // The trace context is deliberately generated here instead of continuing the one for the Update
            // to unlink the asynchronous acs commitment processing from message processing trace.
            TraceContext.createNew("publish_acs_commitment")
          )
        )

    update match {
      // publishing for the CommitSetUpdate-s a CommitSet (or empty if not specified)
      case withCommitSet: CommitSetUpdate =>
        (withCommitSet match {
          case withSequencedCommitSet: CommitSetSequencedUpdate =>
            // throws if a sequenced update has no commit set:
            Some(withSequencedCommitSet.commitSet)
          case withRepairCommitSet: CommitSetRepairUpdate =>
            withRepairCommitSet.repairCommitSetO
        }).foreach {
          case commitSet: CommitSet =>
            val (synchronizerId, synchronizerIndex) = withCommitSet.synchronizerIndex
            publishAcsCommitment(
              synchronizerId,
              synchronizerIndex,
              Some(commitSet),
            )
          case unexpectedCommitSet =>
            ErrorUtil.invalidState(
              s"Unexpected CommitSet of type ${unexpectedCommitSet.getClass.getName} $unexpectedCommitSet"
            )
        }

      case emptyAcsPublicationRequired: EmptyAcsPublicationRequired =>
        val (synchronizerId, synchronizerIndex) = emptyAcsPublicationRequired.synchronizerIndex
        publishAcsCommitment(synchronizerId, synchronizerIndex, commitSetO = None)

      // not publishing otherwise
      case _ => ()
    }
  }

}
