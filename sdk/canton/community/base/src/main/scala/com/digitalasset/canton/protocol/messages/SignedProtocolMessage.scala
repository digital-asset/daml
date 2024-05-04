// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithValidationCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

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
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    // TODO(#12390) Properly check the signatures, i.e. there shouldn't be multiple signatures from the same member on the same envelope
    ClosedEnvelope.verifySignatures(
      snapshot,
      member,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifyMediatorSignatures(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    ClosedEnvelope.verifyMediatorSignatures(
      snapshot,
      mediatorGroupIndex,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifySequencerSignatures(
      snapshot: SyncCryptoApi
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    ClosedEnvelope.verifySequencerSignatures(
      snapshot,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signatures)(representativeProtocolVersion)

  override def domainId: DomainId = message.domainId

  protected def toProtoV30: v30.SignedProtocolMessage = {
    v30.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV30),
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
    extends HasProtocolVersionedWithValidationCompanion[SignedProtocolMessage[
      SignedProtocolMessageContent
    ]] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(
      ProtocolVersion.v31
    )(v30.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
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
    NonEmpty(Seq, signature, moreSignatures*),
    protocolVersion,
  )

  def signAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignedProtocolMessage[M]] = {
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
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] = {
    val hashPurpose = HashPurpose.SignedProtocolMessageSignature
    val serialization = typedMessage.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    cryptoApi.sign(hash)
  }

  def trySignAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[SignedProtocolMessage[M]] =
    signAndCreate(message, cryptoApi, protocolVersion)
      .valueOr(err =>
        throw new IllegalStateException(s"Failed to create signed protocol message: $err")
      )

  private def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      signedMessageP: v30.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v30.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP
    for {
      typedMessage <- TypedSignedProtocolMessageContent.fromByteString(expectedProtocolVersion)(
        typedMessageBytes
      )
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      signedMessage = SignedProtocolMessage(typedMessage, signatures)(rpv)
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
