// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EnvelopeContent,
  ProtocolMessage,
}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens

/** A [[ClosedEnvelope]]'s contents are serialized as a [[com.google.protobuf.ByteString]].
  *
  * The serialization is interpreted as a [[com.digitalasset.canton.protocol.messages.EnvelopeContent]]
  * if `signatures` are empty, and as a [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent]] otherwise.
  * It itself is serialized without version wrappers inside a [[Batch]].
  */
final case class ClosedEnvelope private (
    bytes: ByteString,
    override val recipients: Recipients,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ClosedEnvelope.type])
    extends Envelope[ByteString]
    with HasProtocolVersionedWrapper[ClosedEnvelope] {

  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  @transient override protected lazy val companionObj: ClosedEnvelope.type = ClosedEnvelope

  def openEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope] =
    EnvelopeContent
      .fromByteStringLegacy(protocolVersion)((hashOps, protocolVersion))(bytes)
      .map { envelopeContent =>
        OpenEnvelope(envelopeContent.message, recipients)(protocolVersion)
      }

  override def pretty: Pretty[ClosedEnvelope] = prettyOfClass(
    param("recipients", _.recipients)
  )

  override def forRecipient(
      member: Member
  ): Option[ClosedEnvelope] =
    recipients.forMember(member).map(r => this.copy(recipients = r))

  override def closeEnvelope: this.type = this

  def toProtoV0: v0.Envelope = v0.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV0),
  )

  @VisibleForTesting
  def copy(
      bytes: ByteString = this.bytes,
      recipients: Recipients = this.recipients,
  ): ClosedEnvelope =
    ClosedEnvelope(bytes, recipients)(representativeProtocolVersion)
}

object ClosedEnvelope extends HasProtocolVersionedCompanion[ClosedEnvelope] {

  override type Deserializer = ByteString => ParsingResult[ClosedEnvelope]

  override def name: String = "ClosedEnvelope"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.Envelope)(
      protoCompanion =>
        ProtoConverter.protoParser(protoCompanion.parseFrom)(_).flatMap(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      bytes: ByteString,
      recipients: Recipients,
      protocolVersion: ProtocolVersion,
  ): ClosedEnvelope =
    ClosedEnvelope(bytes, recipients)(protocolVersionRepresentativeFor(protocolVersion))

  private[protocol] def fromProtoV0(envelopeP: v0.Envelope): ParsingResult[ClosedEnvelope] = {
    val v0.Envelope(contentP, recipientsP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(
        Recipients.fromProtoV0(_),
        "recipients",
        recipientsP,
      )
      closedEnvelope = ClosedEnvelope(contentP, recipients)(
        protocolVersionRepresentativeFor(ProtoVersion(0))
      )
    } yield closedEnvelope
  }

  def tryDefaultOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): Lens[ClosedEnvelope, DefaultOpenEnvelope] =
    Lens[ClosedEnvelope, DefaultOpenEnvelope](
      _.openEnvelope(hashOps, protocolVersion).valueOr(err =>
        throw new IllegalArgumentException(s"Failed to open envelope: $err")
      )
    )(newOpenEnvelope => _ => newOpenEnvelope.closeEnvelope)

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): ByteString => ParsingResult[ClosedEnvelope] = _ => Left(error)

  def fromProtocolMessage(
      protocolMessage: ProtocolMessage,
      recipients: Recipients,
      protocolVersion: ProtocolVersion,
  ): ClosedEnvelope =
    ClosedEnvelope(
      EnvelopeContent.tryCreate(protocolMessage, protocolVersion).toByteString,
      recipients,
      protocolVersion,
    )
}
