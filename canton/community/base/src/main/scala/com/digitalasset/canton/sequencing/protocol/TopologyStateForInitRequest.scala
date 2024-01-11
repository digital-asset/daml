// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.*

/** A request to receive the topology state for initialization
  *
  * @param member the member subscribing to the sequencer
  */
final case class TopologyStateForInitRequest(member: Member)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TopologyStateForInitRequest.type
    ]
) extends HasProtocolVersionedWrapper[TopologyStateForInitRequest] {

  @transient override protected lazy val companionObj: TopologyStateForInitRequest.type =
    TopologyStateForInitRequest

  def toProtoV0: v0.TopologyStateForInitRequest =
    v0.TopologyStateForInitRequest(member.toProtoPrimitive)
}

object TopologyStateForInitRequest
    extends HasProtocolVersionedCompanion[TopologyStateForInitRequest] {
  override val name: String = "TopologyStateForInitRequest"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.v3),
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.CNTestNet)(
      v0.TopologyStateForInitRequest
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
  )

  def apply(
      member: Member,
      protocolVersion: ProtocolVersion,
  ): TopologyStateForInitRequest =
    TopologyStateForInitRequest(member)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      topologyStateForInitRequestP: v0.TopologyStateForInitRequest
  ): ParsingResult[TopologyStateForInitRequest] = {
    val v0.TopologyStateForInitRequest(memberP) = topologyStateForInitRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
    } yield TopologyStateForInitRequest(member)(
      protocolVersionRepresentativeFor(ProtoVersion(0))
    )
  }

}
