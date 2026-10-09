// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersHistory,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration

object SequencerUtils {

  /** This method computes how far in the future the maximum sequencing time at a given timestamp is
    * allowed to be based on the `sequencerAggregateSubmissionTimeout` in the dynamic synchronizer
    * parameters. This is required due to a possibility of:
    *   - timeout being long, say 10000 seconds and submissions being accepted with it (e.g.
    *     MST=10001)
    *   - timeout being shortened, say to 100 seconds
    *   - submissions being accepted with the new timeout have a tighter bound (e.g. MST=701) than
    *     the ones accepted with the old timeout (still MST=10001)
    *
    * In practice these timeouts should be short to keep the sequencer memory footprint low, but we
    * take into account the changes in this method to provide full correctness.
    *
    * @param timestamp
    *   a sequencing timestamp where a submission was sequenced
    * @param parameterChanges
    *   all known changes of synchronizer parameters (i.e. output of
    *   `listDynamicSynchronizerParametersChanges`)
    * @return
    */
  def maxSequencingTimeUpperBoundAt(
      timestamp: CantonTimestamp,
      parameterChanges: Seq[DynamicSynchronizerParametersWithValidity],
  ): CantonTimestamp =
    parameterChanges.foldLeft(CantonTimestamp.MinValue) { (previousBound, parameterChanges) =>
      val newBound = if (parameterChanges.validFrom > timestamp) {
        // parameterChanges are from the future, so we keep the previous bound
        previousBound
      } else {
        // We compute here latest possible sequencing time, where parameterChanges apply, and add the timeout
        parameterChanges.validUntil
          .getOrElse(timestamp)
          .min(timestamp)
          .plus(
            parameterChanges.parameters.sequencerAggregateSubmissionTimeout.duration
          )
      }
      if (newBound > previousBound) newBound else previousBound
    }

  /** Computes the offset that the sequencer will add to all events (effectively only timeproofs are
    * allowed) past the synchronizer upgrade time to immediately time out the in-flight requests in
    * the participants and mediators. The base of the calculation is the max possible decision time
    * that can exists in the participant, namely sequencing time + mediator reaction timeout +
    * confirmation response timeout (at that sequencing time).
    * @param upgradeTime
    *   The actual upgrade time, as in Successor.upgradeTime
    * @param parameterChanges
    *   List of synchronizer parameter changes at the effective time of the upgrade announcement.
    * @return
    *   an offset that will time out all in-flight requests in the participants and mediators
    */
  def timeOffsetPastSynchronizerUpgrade(
      upgradeTime: CantonTimestamp,
      parameterChanges: Seq[DynamicSynchronizerParametersWithValidity],
  ): NonNegativeFiniteDuration = {
    val maxTime =
      DynamicSynchronizerParametersHistory.latestDecisionDeadline(parameterChanges, upgradeTime)

    NonNegativeFiniteDuration.tryCreate(maxTime - upgradeTime)
  }
}
