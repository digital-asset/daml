// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.Chain
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.protocol.AggregationRule
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.{ErrorUtil, OptionUtil}

/** Describes an incremental update to the in-flight aggregation state */
final case class InFlightAggregationUpdate(
    freshAggregation: Option[FreshInFlightAggregation],
    aggregatedSenders: Chain[AggregatedSender],
) {
  def tryMerge(
      other: InFlightAggregationUpdate
  )(implicit loggingContext: ErrorLoggingContext): InFlightAggregationUpdate = {
    InFlightAggregationUpdate(
      OptionUtil.mergeWith(this.freshAggregation, other.freshAggregation) {
        (thisFresh, otherFresh) =>
          ErrorUtil.requireState(
            thisFresh == otherFresh,
            s"Cannot merge in-flight aggregation updates with different metadata: $thisFresh vs. $otherFresh",
          )
          thisFresh
      },
      this.aggregatedSenders ++ other.aggregatedSenders,
    )
  }
}

object InFlightAggregationUpdate {
  def sender(
      aggregatedSender: AggregatedSender
  ): InFlightAggregationUpdate =
    InFlightAggregationUpdate(None, Chain.one(aggregatedSender))

  val empty: InFlightAggregationUpdate = InFlightAggregationUpdate(None, Chain.empty)
}

/** The metadata for starting a fresh in-flight aggregation */
final case class FreshInFlightAggregation(
    maxSequencingTimestamp: CantonTimestamp,
    rule: AggregationRule,
)

final case class AggregatedSender(sender: Member, aggregation: AggregationBySender)

object AggregatedSender {
  def apply(
      sender: Member,
      signatures: Seq[Seq[Signature]],
      sequencingTimestamp: CantonTimestamp,
  ): AggregatedSender =
    AggregatedSender(sender, AggregationBySender(sequencingTimestamp, signatures))
}
