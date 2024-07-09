// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.protocol.v30.TypedSignedProtocolMessageContent
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class SetTrafficPurchasedMessage private (
    member: Member,
    serial: PositiveInt,
    totalTrafficPurchased: NonNegativeLong,
    domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SetTrafficPurchasedMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends ProtocolVersionedMemoizedEvidence
    with HasProtocolVersionedWrapper[SetTrafficPurchasedMessage]
    with PrettyPrinting
    with SignedProtocolMessageContent {

  // Only used in security tests, this is not part of the protobuf payload
  override val signingTimestamp: Option[CantonTimestamp] = None

  @transient override protected lazy val companionObj: SetTrafficPurchasedMessage.type =
    SetTrafficPurchasedMessage

  def toProtoV30: v30.SetTrafficPurchasedMessage = {
    v30.SetTrafficPurchasedMessage(
      member = member.toProtoPrimitive,
      serial = serial.value,
      totalTrafficPurchased = totalTrafficPurchased.value,
      domainId = domainId.toProtoPrimitive,
    )
  }

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.SetTrafficPurchased(
      getCryptographicEvidence
    )

  override def pretty: Pretty[SetTrafficPurchasedMessage] = prettyOfClass(
    param("member", _.member),
    param("serial", _.serial),
    param("totalTrafficPurchased", _.totalTrafficPurchased),
    param("domainId", _.domainId),
  )
}

object SetTrafficPurchasedMessage
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      SetTrafficPurchasedMessage,
    ] {
  override val name: String = "SetTrafficPurchasedMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v31)(
      v30.SetTrafficPurchasedMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): SetTrafficPurchasedMessage =
    SetTrafficPurchasedMessage(member, serial, totalTrafficPurchased, domainId)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def fromProtoV30(
      proto: v30.SetTrafficPurchasedMessage
  )(bytes: ByteString): ParsingResult[SetTrafficPurchasedMessage] = {
    for {
      member <- Member.fromProtoPrimitive(proto.member, "member")
      serial <- ProtoConverter.parsePositiveInt("serial", proto.serial)
      totalTrafficPurchased <- ProtoConverter.parseNonNegativeLong(
        "total_traffic_purchased",
        proto.totalTrafficPurchased,
      )
      domainId <- DomainId.fromProtoPrimitive(proto.domainId, "domain_id")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(1))
    } yield SetTrafficPurchasedMessage(
      member,
      serial,
      totalTrafficPurchased,
      domainId,
    )(
      rpv,
      Some(bytes),
    )
  }

  implicit val setTrafficPurchasedCast: SignedMessageContentCast[SetTrafficPurchasedMessage] =
    SignedMessageContentCast.create[SetTrafficPurchasedMessage](
      "SetTrafficPurchasedMessage"
    ) {
      case m: SetTrafficPurchasedMessage => Some(m)
      case _ => None
    }
}
