// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.traffic

import cats.implicits.toTraverseOps
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

final case class LSUTrafficState(membersTraffic: Map[Member, TrafficState])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LSUTrafficState.type]
) extends HasProtocolVersionedWrapper[LSUTrafficState] {

  @transient override protected lazy val companionObj: LSUTrafficState.type = LSUTrafficState

  def toProtoV30: v30.LSUTrafficState = {
    val membersTrafficProto = membersTraffic.map { case (member, state) =>
      member.toProtoPrimitive -> state.toProtoV30
    }
    v30.LSUTrafficState(membersTrafficProto)
  }
}

object LSUTrafficState extends VersioningCompanion[LSUTrafficState] {
  override def name: String = "lsu traffic state"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.LSUTrafficState)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def fromProtoV30(
      request: v30.LSUTrafficState
  ): ParsingResult[LSUTrafficState] =
    request.lsuTrafficStates.toList
      .traverse { case (memberP, stateP) =>
        for {
          member <- Member.fromProtoPrimitive(memberP, "member")
          state <- TrafficState.fromProtoV30(stateP)
        } yield member -> state
      }
      .map(trafficState =>
        LSUTrafficState(trafficState.toMap)(versioningTable.table(ProtoVersion(30)))
      )
}
