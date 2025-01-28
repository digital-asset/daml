// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.*

/** A request to receive the topology state for initialization
  *
  * @param member the member subscribing to the sequencer
  */
final case class GetTrafficStateForMemberRequest private (
    member: Member,
    timestamp: CantonTimestamp,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      GetTrafficStateForMemberRequest.type
    ]
) extends HasProtocolVersionedWrapper[GetTrafficStateForMemberRequest] {

  @transient override protected lazy val companionObj: GetTrafficStateForMemberRequest.type =
    GetTrafficStateForMemberRequest

  def toProtoV30: v30.GetTrafficStateForMemberRequest =
    v30.GetTrafficStateForMemberRequest(member.toProtoPrimitive, timestamp.toProtoPrimitive)
}

object GetTrafficStateForMemberRequest
    extends VersioningCompanion[GetTrafficStateForMemberRequest] {
  override val name: String = "GetTrafficStateForMemberRequest"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(
      v30.GetTrafficStateForMemberRequest
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply(
      member: Member,
      timestamp: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  ): GetTrafficStateForMemberRequest =
    GetTrafficStateForMemberRequest(member, timestamp)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      getTrafficStateForMemberRequestP: v30.GetTrafficStateForMemberRequest
  ): ParsingResult[GetTrafficStateForMemberRequest] = {
    val v30.GetTrafficStateForMemberRequest(memberP, timestampP) = getTrafficStateForMemberRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      timestamp <- CantonTimestamp.fromProtoPrimitive(timestampP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield GetTrafficStateForMemberRequest(member, timestamp)(rpv)
  }

}
