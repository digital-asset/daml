// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update.EmptyAcsPublicationRequired
import com.digitalasset.canton.ledger.participant.state.{CommitSetUpdate, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.sync.ConnectedDomainsLookupContainer
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

class AcsCommitmentPublicationPostProcessor(
    connectedDomainsLookupContainer: ConnectedDomainsLookupContainer,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with (Update => Unit) {

  def apply(update: Update): Unit = {
    implicit val tc: TraceContext = update.traceContext
    def publishAcsCommitment(
        domainId: DomainId,
        sequencerTimestamp: CantonTimestamp,
        requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)],
    ): Unit =
      connectedDomainsLookupContainer
        // not publishing if no domain active: it means subsequent crash recovery will establish consistency again
        .get(domainId)
        // not publishing anything if the AcsCommitmentProcessor initialization succeeded with AbortedDueToShutdown or failed
        .foreach(
          _.acsCommitmentProcessor.publish(
            sequencerTimestamp,
            requestCounterCommitSetPairO,
          )(
            // The trace context is deliberately generated here instead of continuing the one for the Update
            // to unlink the asynchronous acs commitment processing from message processing trace.
            TraceContext.createNew()
          )
        )
    update match {
      // publishing for the CommitSetUpdate-s a CommitSet (or empty if not specified)
      case withCommitSet: CommitSetUpdate =>
        publishAcsCommitment(
          domainId = withCommitSet.domainId,
          sequencerTimestamp = withCommitSet.recordTime,
          requestCounterCommitSetPairO = withCommitSet.commitSet match {
            case commitSet: CommitSet => Some(withCommitSet.requestCounter -> commitSet)
            case unexpectedCommitSet =>
              ErrorUtil.invalidState(
                s"Unexpected CommitSet of type ${unexpectedCommitSet.getClass.getName} $unexpectedCommitSet"
              )
          },
        )

      case emptyAcsPublicationRequired: EmptyAcsPublicationRequired =>
        publishAcsCommitment(
          domainId = emptyAcsPublicationRequired.domainId,
          sequencerTimestamp = emptyAcsPublicationRequired.recordTime,
          requestCounterCommitSetPairO = None,
        )

      // not publishing otherwise
      case _ => ()
    }
  }

}
