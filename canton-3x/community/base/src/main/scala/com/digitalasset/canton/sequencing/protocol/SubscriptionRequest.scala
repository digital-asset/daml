// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

/** A request to receive events from a given counter from a sequencer.
  *
  * @param member the member subscribing to the sequencer
  * @param counter the counter of the first event to receive.
  */
final case class SubscriptionRequest(member: Member, counter: SequencerCounter)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubscriptionRequest.type
    ]
) extends HasProtocolVersionedWrapper[SubscriptionRequest] {

  @transient override protected lazy val companionObj: SubscriptionRequest.type =
    SubscriptionRequest

  def toProtoV0: v0.SubscriptionRequest = v0.SubscriptionRequest(member.toProtoPrimitive, counter.v)
}

object SubscriptionRequest extends HasProtocolVersionedCompanion[SubscriptionRequest] {
  override val name: String = "SubscriptionRequest"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v30)(v0.SubscriptionRequest)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      member: Member,
      counter: SequencerCounter,
      protocolVersion: ProtocolVersion,
  ): SubscriptionRequest =
    SubscriptionRequest(member, counter)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      subscriptionRequestP: v0.SubscriptionRequest
  ): ParsingResult[SubscriptionRequest] = {
    val v0.SubscriptionRequest(memberP, counter) = subscriptionRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
    } yield SubscriptionRequest(member, SequencerCounter(counter))(
      protocolVersionRepresentativeFor(ProtoVersion(0))
    )
  }

}
