// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.domain.api.v30
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

  def toProtoV30: v30.DownloadTopologyStateForInitRequest =
    v30.DownloadTopologyStateForInitRequest(member.toProtoPrimitive)
}

object TopologyStateForInitRequest
    extends HasProtocolVersionedCompanion[TopologyStateForInitRequest] {
  override val name: String = "TopologyStateForInitRequest"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.DownloadTopologyStateForInitRequest
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      member: Member,
      protocolVersion: ProtocolVersion,
  ): TopologyStateForInitRequest =
    TopologyStateForInitRequest(member)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV30(
      topologyStateForInitRequestP: v30.DownloadTopologyStateForInitRequest
  ): ParsingResult[TopologyStateForInitRequest] = {
    val v30.DownloadTopologyStateForInitRequest(memberP) = topologyStateForInitRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TopologyStateForInitRequest(member)(rpv)
  }

}
