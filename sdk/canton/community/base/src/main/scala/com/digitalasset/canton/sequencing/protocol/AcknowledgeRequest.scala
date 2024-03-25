// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

final case class AcknowledgeRequest private (member: Member, timestamp: CantonTimestamp)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcknowledgeRequest.type
    ],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[AcknowledgeRequest]
    with ProtocolVersionedMemoizedEvidence {
  def toProtoV30: v30.AcknowledgeRequest =
    v30.AcknowledgeRequest(member.toProtoPrimitive, timestamp.toProtoPrimitive)

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: AcknowledgeRequest.type = AcknowledgeRequest
}

object AcknowledgeRequest extends HasMemoizedProtocolVersionedWrapperCompanion[AcknowledgeRequest] {
  def apply(
      member: Member,
      timestamp: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  ): AcknowledgeRequest =
    AcknowledgeRequest(member, timestamp)(protocolVersionRepresentativeFor(protocolVersion))

  override def name: String = "AcknowledgeRequest"

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.AcknowledgeRequest)(
        supportedProtoVersionMemoized(_) { req => bytes =>
          fromProtoV30(req)(Some(bytes))
        },
        _.toProtoV30.toByteString,
      )
    )

  private def fromProtoV30(
      reqP: v30.AcknowledgeRequest
  )(deserializedFrom: Option[ByteString]): ParsingResult[AcknowledgeRequest] =
    for {
      member <- Member.fromProtoPrimitive(reqP.member, "member")
      timestamp <- CantonTimestamp.fromProtoPrimitive(reqP.timestamp)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield {
      AcknowledgeRequest(member, timestamp)(
        rpv,
        deserializedFrom,
      )
    }
}
