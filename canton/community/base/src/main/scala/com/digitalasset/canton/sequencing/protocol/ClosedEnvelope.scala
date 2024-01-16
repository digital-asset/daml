// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.InvariantViolation
import com.digitalasset.canton.crypto.{
  HashOps,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SyncCryptoApi,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EnvelopeContent,
  ProtocolMessage,
  SignedProtocolMessage,
  TypedSignedProtocolMessageContent,
}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
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

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered

/** A [[ClosedEnvelope]]'s contents are serialized as a [[com.google.protobuf.ByteString]].
  *
  * The serialization is interpreted as a [[com.digitalasset.canton.protocol.messages.EnvelopeContent]]
  * if `signatures` are empty, and as a [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent]] otherwise.
  * It itself is serialized without version wrappers inside a [[Batch]].
  */
final case class ClosedEnvelope private (
    bytes: ByteString,
    override val recipients: Recipients,
    signatures: Seq[Signature],
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ClosedEnvelope.type])
    extends Envelope[ByteString]
    with HasProtocolVersionedWrapper[ClosedEnvelope] {

  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  @transient override protected lazy val companionObj: ClosedEnvelope.type = ClosedEnvelope

  def openEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope] = {
    NonEmpty.from(signatures) match {
      case None =>
        EnvelopeContent
          .fromByteStringLegacy(protocolVersion)((hashOps, protocolVersion))(bytes)
          .map { envelopeContent =>
            OpenEnvelope(envelopeContent.message, recipients)(protocolVersion)
          }
      case Some(signaturesNE) =>
        TypedSignedProtocolMessageContent
          .fromByteString(hashOps, protocolVersion)(bytes)
          .map { typedMessage =>
            OpenEnvelope(
              SignedProtocolMessage.tryCreate(typedMessage, signaturesNE, protocolVersion),
              recipients,
            )(protocolVersion)
          }
    }
  }

  override def pretty: Pretty[ClosedEnvelope] = prettyOfClass(
    param("recipients", _.recipients),
    paramIfNonEmpty("signatures", _.signatures),
  )

  override def forRecipient(
      member: Member,
      groupRecipients: Set[GroupRecipient],
  ): Option[ClosedEnvelope] =
    recipients.forMember(member, groupRecipients).map(r => this.copy(recipients = r))

  override def closeEnvelope: this.type = this

  def toProtoV0: v0.Envelope = v0.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV0),
  )

  def toProtoV1: v1.Envelope = v1.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV0),
    signatures = signatures.map(_.toProtoV0),
  )

  @VisibleForTesting
  def copy(
      bytes: ByteString = this.bytes,
      recipients: Recipients = this.recipients,
      signatures: Seq[Signature] = this.signatures,
  ): ClosedEnvelope =
    ClosedEnvelope
      .create(bytes, recipients, signatures, representativeProtocolVersion)
      .valueOr(error => throw new IllegalArgumentException(error.message))

  def verifySignatures(
      snapshot: SyncCryptoApi,
      sender: Member,
  )(implicit ec: ExecutionContext): EitherT[Future, SignatureCheckError, Unit] = {
    NonEmpty
      .from(signatures)
      .traverse_(ClosedEnvelope.verifySignatures(snapshot, sender, bytes, _))
  }
}

object ClosedEnvelope extends HasProtocolVersionedCompanion[ClosedEnvelope] {

  override type Deserializer = ByteString => ParsingResult[ClosedEnvelope]

  override def name: String = "ClosedEnvelope"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.Envelope)(
      protoCompanion =>
        ProtoConverter.protoParser(protoCompanion.parseFrom)(_).flatMap(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.CNTestNet
    )(v1.Envelope)(
      protoCompanion =>
        ProtoConverter.protoParser(protoCompanion.parseFrom)(_).flatMap(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  private val signaturesSupportedSince = protocolVersionRepresentativeFor(ProtoVersion(1))
  private[protocol] val groupAddressesSupportedSince = protocolVersionRepresentativeFor(
    ProtoVersion(1)
  )

  override lazy val invariants = Seq(defaultSignaturesUntil)

  lazy val defaultSignaturesUntil = DefaultValueUntilExclusive(
    _.signatures,
    "signatures",
    protocolVersionRepresentativeFor(ProtoVersion(1)),
    Nil,
  )

  def create(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      representativeProtocolVersion: RepresentativeProtocolVersion[ClosedEnvelope.type],
  ): Either[InvariantViolation, ClosedEnvelope] = {
    for {
      _ <- Either.cond(
        signatures.isEmpty || representativeProtocolVersion >= signaturesSupportedSince,
        (),
        InvariantViolation(
          s"Signatures on closed envelopes are supported only from protocol version ${signaturesSupportedSince} on."
        ),
      )
      _ <- Either.cond(
        representativeProtocolVersion >= groupAddressesSupportedSince || !recipients.allRecipients
          .exists {
            case _: MemberRecipient => false
            case _ => true
          },
        (),
        InvariantViolation(
          s"Group addresses on closed envelopes are supported only from protocol version ${signaturesSupportedSince} on."
        ),
      )
    } yield new ClosedEnvelope(bytes, recipients, signatures)(representativeProtocolVersion)
  }

  def create(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      protocolVersion: ProtocolVersion,
  ): Either[InvariantViolation, ClosedEnvelope] =
    create(bytes, recipients, signatures, protocolVersionRepresentativeFor(protocolVersion))

  def tryCreate(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      protocolVersion: ProtocolVersion,
  ): ClosedEnvelope =
    create(bytes, recipients, signatures, protocolVersion).valueOr(error =>
      throw new IllegalArgumentException(error.message)
    )

  private[protocol] def fromProtoV0(envelopeP: v0.Envelope): ParsingResult[ClosedEnvelope] = {
    val v0.Envelope(contentP, recipientsP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(
        Recipients.fromProtoV0(_, supportGroupAddressing = false),
        "recipients",
        recipientsP,
      )
      closedEnvelope <- ClosedEnvelope
        .create(
          contentP,
          recipients,
          Seq.empty,
          protocolVersionRepresentativeFor(ProtoVersion(0)),
        )
        .leftMap(ProtoDeserializationError.InvariantViolation.toProtoDeserializationError)
    } yield closedEnvelope
  }

  private[protocol] def fromProtoV1(envelopeP: v1.Envelope): ParsingResult[ClosedEnvelope] = {
    val v1.Envelope(contentP, recipientsP, signaturesP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(
        Recipients.fromProtoV0(_, supportGroupAddressing = true),
        "recipients",
        recipientsP,
      )
      signatures <- signaturesP.traverse(Signature.fromProtoV0)
      closedEnvelope <- ClosedEnvelope
        .create(
          contentP,
          recipients,
          signatures,
          protocolVersionRepresentativeFor(ProtoVersion(1)),
        )
        .leftMap(ProtoDeserializationError.InvariantViolation.toProtoDeserializationError)
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
  ): ClosedEnvelope = {
    if (protocolVersionRepresentativeFor(protocolVersion) >= signaturesSupportedSince) {
      protocolMessage match {
        case SignedProtocolMessage(typedMessageContent, signatures) =>
          ClosedEnvelope.tryCreate(
            typedMessageContent.toByteString,
            recipients,
            signatures,
            protocolVersion,
          )
        case _ =>
          ClosedEnvelope.tryCreate(
            EnvelopeContent.tryCreate(protocolMessage, protocolVersion).toByteString,
            recipients,
            Seq.empty,
            protocolVersion,
          )
      }
    } else {
      ClosedEnvelope.tryCreate(
        EnvelopeContent.tryCreate(protocolMessage, protocolVersion).toByteString,
        recipients,
        Seq.empty,
        protocolVersion,
      )
    }
  }

  // TODO(#15153) Remove comment
  /** Use only in protocol version [[com.digitalasset.canton.version.ProtocolVersion.CNTestNet]] or later */
  def verifySignatures(
      snapshot: SyncCryptoApi,
      sender: Member,
      content: ByteString,
      signatures: NonEmpty[Seq[Signature]],
  ): EitherT[Future, SignatureCheckError, Unit] = {
    val hash = snapshot.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, content)
    snapshot.verifySignatures(hash, sender, signatures)
  }

  // TODO(#15153) Remove comment
  /** Use only in protocol version [[com.digitalasset.canton.version.ProtocolVersion.CNTestNet]] or later */
  def verifySignatures(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
      content: ByteString,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] = {
    val hash = snapshot.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, content)
    snapshot.verifySignatures(hash, mediatorGroupIndex, signatures)
  }
}
