// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SignatureCheckError,
  SyncCryptoApi,
  SyncCryptoError,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v1
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered

/** There can be any number of signatures.
  * Every signature covers the serialization of the `typedMessage` and needs to be valid.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedProtocolMessage[+M <: SignedProtocolMessageContent](
    typedMessage: TypedSignedProtocolMessageContent[M],
    signatures: NonEmpty[Seq[Signature]],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedProtocolMessage.type
    ]
) extends ProtocolMessage
    with HasProtocolVersionedWrapper[SignedProtocolMessage[SignedProtocolMessageContent]] {

  @transient override protected lazy val companionObj: SignedProtocolMessage.type =
    SignedProtocolMessage

  def message: M = typedMessage.content

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
  ): EitherT[Future, SignatureCheckError, Unit] =
    if (
      representativeProtocolVersion >=
        companionObj.protocolVersionRepresentativeFor(ProtocolVersion.v30)
    ) {
      // TODO(#12390) Properly check the signatures, i.e. there shouldn't be multiple signatures from the same member on the same envelope
      ClosedEnvelope.verifySignatures(
        snapshot,
        member,
        typedMessage.getCryptographicEvidence,
        signatures,
      )
    } else {
      val hashPurpose = message.hashPurpose
      val hash = snapshot.pureCrypto.digest(hashPurpose, message.getCryptographicEvidence)
      snapshot.verifySignatures(hash, member, signatures)
    }

  def verifySignature(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] = {
    if (
      representativeProtocolVersion >=
        companionObj.protocolVersionRepresentativeFor(ProtocolVersion.v30)
    ) {

      ClosedEnvelope.verifySignatures(
        snapshot,
        mediatorGroupIndex,
        typedMessage.getCryptographicEvidence,
        signatures,
      )
    } else {
      val hashPurpose = message.hashPurpose
      val hash = snapshot.pureCrypto.digest(hashPurpose, message.getCryptographicEvidence)
      snapshot.verifySignatures(hash, mediatorGroupIndex, signatures)
    }
  }

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signatures)(representativeProtocolVersion)

  override def domainId: DomainId = message.domainId

  protected def toProtoV1: v1.SignedProtocolMessage = {
    v1.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV0),
      typedSignedProtocolMessageContent = typedMessage.toByteString,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] = {
    F.map(typedMessage.traverse(f)) { newTypedMessage =>
      if (newTypedMessage eq typedMessage) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(typedMessage = newTypedMessage)
    }
  }

  override def pretty: Pretty[this.type] =
    prettyOfClass(unnamedParam(_.message), param("signatures", _.signatures))
}

object SignedProtocolMessage
    extends HasProtocolVersionedCompanion[SignedProtocolMessage[
      SignedProtocolMessageContent
    ]] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v30
    )(v1.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    )
  )

  def apply[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): SignedProtocolMessage[M] =
    SignedProtocolMessage(typedMessage, signatures)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  @VisibleForTesting
  def from[M <: SignedProtocolMessageContent](
      message: M,
      protocolVersion: ProtocolVersion,
      signature: Signature,
      moreSignatures: Signature*
  ): SignedProtocolMessage[M] = SignedProtocolMessage(
    TypedSignedProtocolMessageContent(message, protocolVersion),
    NonEmpty(Seq, signature, moreSignatures: _*),
    protocolVersion,
  )

  def signAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, SignedProtocolMessage[M]] = {
    val typedMessage = TypedSignedProtocolMessageContent(message, protocolVersion)
    for {
      signature <- mkSignature(typedMessage, cryptoApi)
    } yield SignedProtocolMessage(typedMessage, NonEmpty(Seq, signature))(
      protocolVersionRepresentativeFor(protocolVersion)
    )
  }

  @VisibleForTesting
  private[canton] def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature] = {
    val hashPurpose = HashPurpose.SignedProtocolMessageSignature
    val serialization = typedMessage.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    cryptoApi.sign(hash)
  }

  def trySignAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[SignedProtocolMessage[M]] =
    signAndCreate(message, cryptoApi, protocolVersion)
      .valueOr(err =>
        throw new IllegalStateException(s"Failed to create signed protocol message: $err")
      )

  private def fromProtoV1(
      signedMessageP: v1.SignedProtocolMessage
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v1.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP
    for {
      typedMessage <- TypedSignedProtocolMessageContent.fromByteString(typedMessageBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV0,
        "signatures",
        signaturesP,
      )
      signedMessage = SignedProtocolMessage(typedMessage, signatures)(
        protocolVersionRepresentativeFor(ProtoVersion(1))
      )
    } yield signedMessage
  }

  implicit def signedMessageCast[M <: SignedProtocolMessageContent](implicit
      cast: SignedMessageContentCast[M]
  ): ProtocolMessageContentCast[SignedProtocolMessage[M]] =
    ProtocolMessageContentCast.create[SignedProtocolMessage[M]](cast.targetKind) {
      case sm: SignedProtocolMessage[SignedProtocolMessageContent] => sm.traverse(cast.toKind)
      case _ => None
    }
}
