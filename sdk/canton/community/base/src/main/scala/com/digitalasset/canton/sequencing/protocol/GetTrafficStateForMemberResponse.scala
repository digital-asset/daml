// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

/** A response from the GetTrafficForMember RPC
  *
  * @param trafficState the traffic state of the requested member, if any
  */
final case class GetTrafficStateForMemberResponse private (trafficState: Option[TrafficState])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      GetTrafficStateForMemberResponse.type
    ]
) extends HasProtocolVersionedWrapper[GetTrafficStateForMemberResponse] {

  @transient override protected lazy val companionObj: GetTrafficStateForMemberResponse.type =
    GetTrafficStateForMemberResponse

  def toProtoV30: v30.GetTrafficStateForMemberResponse =
    v30.GetTrafficStateForMemberResponse(trafficState.map(_.toProtoV30))
}

object GetTrafficStateForMemberResponse
    extends HasProtocolVersionedCompanion[GetTrafficStateForMemberResponse] {
  override val name: String = "GetTrafficStateForMemberResponse"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
      v30.GetTrafficStateForMemberResponse
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      trafficState: Option[TrafficState],
      protocolVersion: ProtocolVersion,
  ): GetTrafficStateForMemberResponse =
    GetTrafficStateForMemberResponse(trafficState)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      response: v30.GetTrafficStateForMemberResponse
  ): ParsingResult[GetTrafficStateForMemberResponse] = {
    val v30.GetTrafficStateForMemberResponse(trafficStateP) = response
    for {
      trafficStateO <- trafficStateP.traverse(TrafficState.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield GetTrafficStateForMemberResponse(trafficStateO)(rpv)
  }

}
