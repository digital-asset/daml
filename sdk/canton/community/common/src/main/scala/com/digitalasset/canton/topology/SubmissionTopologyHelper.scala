// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

object SubmissionTopologyHelper {

  /** Retrieve the topology snapshot used during submission by the submitter of a confirmation request.
    * This can be used to determine the impact of a topology change between submission and sequencing.
    * An example usage is during validation of a request: if some validation fails due to such a change,
    * the severity of the logs can sometimes be lowered from warning to info.
    *
    * Return `None` if the timestamp of the topology snapshot used at submission is too far in the past
    * compared to sequencing time (as determined by
    * [[com.digitalasset.canton.config.ProcessingTimeout.topologyChangeWarnDelay]]).
    *
    * @param sequencingTimestamp the timestamp at which the request was sequenced
    * @param submissionTopologyTimestamp the timestamp of the topology used at submission
    */
  def getSubmissionTopologySnapshot(
      timeouts: ProcessingTimeout,
      sequencingTimestamp: CantonTimestamp,
      submissionTopologyTimestamp: CantonTimestamp,
      crypto: DomainSyncCryptoClient,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[TopologySnapshot]] = {
    val maxDelay = timeouts.topologyChangeWarnDelay.toInternal
    val minTs = sequencingTimestamp - maxDelay

    if (submissionTopologyTimestamp >= minTs)
      crypto
        .awaitSnapshotUSSupervised(s"await crypto snapshot $submissionTopologyTimestamp")(
          submissionTopologyTimestamp
        )
        .map(syncCryptoApi => Some(syncCryptoApi.ipsSnapshot))
    else {
      logger.info(
        s"""Declared submission topology timestamp $submissionTopologyTimestamp is too far in the past (minimum
           |accepted: $minTs). Ignoring.
           |Note: this delay can be adjusted with the `topology-change-warn-delay` configuration parameter.""".stripMargin
          .replaceAll("\n", " ")
      )
      FutureUnlessShutdown.pure(None)
    }
  }
}
