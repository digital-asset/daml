// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
final case class SubscriptionRequestV2(member: Member, timestamp: Option[CantonTimestamp])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubscriptionRequestV2.type
    ]
) extends HasProtocolVersionedWrapper[SubscriptionRequestV2] {

  @transient override protected lazy val companionObj: SubscriptionRequestV2.type =
    SubscriptionRequestV2

  def toProtoV30: v30.SubscriptionRequestV2 =
    v30.SubscriptionRequestV2(member.toProtoPrimitive, timestamp.map(_.toProtoPrimitive))
}

object SubscriptionRequestV2 extends VersioningCompanion[SubscriptionRequestV2] {
  override val name: String = "SubscriptionRequestV2"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.SubscriptionRequestV2)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply(
      member: Member,
      timestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SubscriptionRequestV2 =
    SubscriptionRequestV2(member, timestamp)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV30(
      subscriptionRequestP: v30.SubscriptionRequestV2
  ): ParsingResult[SubscriptionRequestV2] = {
    val v30.SubscriptionRequestV2(memberP, timestampOP) = subscriptionRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      timestamp <- timestampOP.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SubscriptionRequestV2(member, timestamp)(rpv)
  }
}
