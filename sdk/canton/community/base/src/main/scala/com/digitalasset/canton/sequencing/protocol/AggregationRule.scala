// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  RepresentativeProtocolVersion,
}

/** Encodes the conditions on when an aggregatable submission request's envelopes are sequenced and delivered.
  *
  * Aggregatable submissions are grouped by their [[SubmissionRequest.aggregationId]].
  * An aggregatable submission's envelopes are delivered to their recipients when the [[threshold]]'s
  * submission request in its group has been sequenced. The aggregatable submission request that triggers the threshold
  * defines the sequencing timestamp (and thus the sequencer counters) for all delivered envelopes.
  * The sender of an aggregatable submission request receives a receipt of delivery immediately when its request was sequenced,
  * not when its envelopes were delivered. When the envelopes are actually delivered, no further delivery receipt is sent.
  *
  * So a threshold of 1 means that no aggregation takes place and the event is sequenced immediately.
  * In this case, one can completely omit the aggregation rule in the submission request.
  */
final case class AggregationRule(
    // TODO(#12075) This is a `Seq` rather than a `Set` just because we then have to worry less about deterministic serialization.
    //  Change it to a set.
    eligibleSenders: NonEmpty[Seq[Member]],
    threshold: PositiveInt,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[AggregationRule.type]
) extends HasProtocolVersionedWrapper[AggregationRule]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: AggregationRule.type = AggregationRule

  private[canton] def toProtoV30: v30.AggregationRule = v30.AggregationRule(
    eligibleMembers = eligibleSenders.map(_.toProtoPrimitive),
    threshold = threshold.value,
  )

  override def pretty: Pretty[this.type] = prettyOfClass(
    param("threshold", _.threshold),
    param("eligible members", _.eligibleSenders),
  )

  def copy(
      eligibleMembers: NonEmpty[Seq[Member]] = this.eligibleSenders,
      threshold: PositiveInt = this.threshold,
  ): AggregationRule =
    AggregationRule(eligibleMembers, threshold)(representativeProtocolVersion)
}

object AggregationRule
    extends HasProtocolVersionedCompanion[AggregationRule]
    with ProtocolVersionedCompanionDbHelpers[AggregationRule] {
  def apply(
      eligibleMembers: NonEmpty[Seq[Member]],
      threshold: PositiveInt,
      protocolVersion: ProtocolVersion,
  ): AggregationRule =
    AggregationRule(eligibleMembers, threshold)(protocolVersionRepresentativeFor(protocolVersion))

  override def name: String = "AggregationRule"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.AggregationRule)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[canton] def fromProtoV30(proto: v30.AggregationRule): ParsingResult[AggregationRule] = {
    val v30.AggregationRule(eligibleMembersP, thresholdP) = proto
    for {
      eligibleMembers <- ProtoConverter.parseRequiredNonEmpty(
        Member.fromProtoPrimitive(_, "eligible_members"),
        "eligible_members",
        eligibleMembersP,
      )
      threshold <- ProtoConverter.parsePositiveInt(thresholdP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield AggregationRule(eligibleMembers, threshold)(rpv)
  }
}
