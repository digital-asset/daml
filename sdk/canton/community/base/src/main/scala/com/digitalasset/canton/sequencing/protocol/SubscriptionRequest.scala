// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.implicits.toTraverseOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.*

/** A request to receive events from a given counter from a sequencer.
  *
  * @param member
  *   the member subscribing to the sequencer
  * @param timestamp
  *   inclusive lower bound for the timestamp of the events to receive.
  */
final case class SubscriptionRequest(member: Member, timestamp: Option[CantonTimestamp])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubscriptionRequest.type
    ]
) extends HasProtocolVersionedWrapper[SubscriptionRequest] {

  @transient override protected lazy val companionObj: SubscriptionRequest.type =
    SubscriptionRequest

  def toProtoV30: v30.SubscriptionRequest =
    v30.SubscriptionRequest(member.toProtoPrimitive, timestamp.map(_.toProtoPrimitive))
}

object SubscriptionRequest extends VersioningCompanion[SubscriptionRequest] {
  override val name: String = "SubscriptionRequestV2"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.SubscriptionRequest)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply(
      member: Member,
      timestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SubscriptionRequest =
    SubscriptionRequest(member, timestamp)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV30(
      subscriptionRequestP: v30.SubscriptionRequest
  ): ParsingResult[SubscriptionRequest] = {
    val v30.SubscriptionRequest(memberP, timestampOP) = subscriptionRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      timestamp <- timestampOP.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SubscriptionRequest(member, timestamp)(rpv)
  }
}
