// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
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

final case class SetTrafficBalanceMessage private (
    member: Member,
    serial: NonNegativeLong,
    totalTrafficBalance: NonNegativeLong,
    domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SetTrafficBalanceMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends ProtocolVersionedMemoizedEvidence
    with HasProtocolVersionedWrapper[SetTrafficBalanceMessage]
    with PrettyPrinting
    with SignedProtocolMessageContent {

  // Only used in security tests, this is not part of the protobuf payload
  override val signingTimestamp: Option[CantonTimestamp] = None

  @transient override protected lazy val companionObj: SetTrafficBalanceMessage.type =
    SetTrafficBalanceMessage

  def toProtoV30: v30.SetTrafficBalanceMessage = {
    v30.SetTrafficBalanceMessage(
      member = member.toProtoPrimitive,
      serial = serial.value,
      totalTrafficBalance = totalTrafficBalance.value,
      domainId = domainId.toProtoPrimitive,
    )
  }

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.SetTrafficBalance(
      getCryptographicEvidence
    )

  override def pretty: Pretty[SetTrafficBalanceMessage] = prettyOfClass(
    param("member", _.member),
    param("serial", _.serial),
    param("totalTrafficBalance", _.totalTrafficBalance),
    param("domainId", _.domainId),
  )
}

object SetTrafficBalanceMessage
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      SetTrafficBalanceMessage,
    ] {
  override val name: String = "SetTrafficBalanceMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.SetTrafficBalanceMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      member: Member,
      serial: NonNegativeLong,
      totalTrafficBalance: NonNegativeLong,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): SetTrafficBalanceMessage =
    SetTrafficBalanceMessage(member, serial, totalTrafficBalance, domainId)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def fromProtoV30(
      proto: v30.SetTrafficBalanceMessage
  )(bytes: ByteString): ParsingResult[SetTrafficBalanceMessage] = {
    for {
      member <- Member.fromProtoPrimitive(proto.member, "member")
      serial <- ProtoConverter.parseNonNegativeLong(proto.serial)
      totalTrafficBalance <- ProtoConverter.parseNonNegativeLong(proto.totalTrafficBalance)
      domainId <- DomainId.fromProtoPrimitive(proto.domainId, "domain_id")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(1))
    } yield SetTrafficBalanceMessage(
      member,
      serial,
      totalTrafficBalance,
      domainId,
    )(
      rpv,
      Some(bytes),
    )
  }

  implicit val setTrafficBalanceCast: SignedMessageContentCast[SetTrafficBalanceMessage] =
    SignedMessageContentCast.create[SetTrafficBalanceMessage](
      "SetTrafficBalanceMessage"
    ) {
      case m: SetTrafficBalanceMessage => Some(m)
      case _ => None
    }
}
