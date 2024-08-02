// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.{
  HashOps,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SyncCryptoApi,
  SyncCryptoError,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextAndValidationCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

// sealed because this class is mocked in tests
sealed case class SignedProtocolMessage[+M <: SignedProtocolMessageContent] private (
    typedMessage: TypedSignedProtocolMessageContent[M],
    signature: Signature,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedProtocolMessage.type
    ]
) extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with ProtocolMessageV3
    with HasProtocolVersionedWrapper[SignedProtocolMessage[SignedProtocolMessageContent]] {

  @transient override protected lazy val companionObj: SignedProtocolMessage.type =
    SignedProtocolMessage

  def message: M = typedMessage.content

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
  ): EitherT[Future, SignatureCheckError, Unit] = {
    val hashPurpose = message.hashPurpose
    val hash = snapshot.pureCrypto.digest(hashPurpose, message.getCryptographicEvidence)
    snapshot.verifySignatures(hash, member, NonEmpty.mk(Seq, signature))
  }

  def verifySignature(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] = {
    val hashPurpose = message.hashPurpose
    val hash = snapshot.pureCrypto.digest(hashPurpose, message.getCryptographicEvidence)
    snapshot.verifySignatures(hash, mediatorGroupIndex, NonEmpty.mk(Seq, signature))
  }

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signature: Signature = this.signature,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signature)(representativeProtocolVersion)

  override def domainId: DomainId = message.domainId

  protected def toProtoV0: v0.SignedProtocolMessage = {
    val content = typedMessage.content.toProtoSomeSignedProtocolMessage
    v0.SignedProtocolMessage(
      signature = signature.toProtoV0.some,
      someSignedProtocolMessage = content,
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(v2.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(v3.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] =
    F.map(typedMessage.traverse(f)) { newTypedMessage =>
      if (newTypedMessage eq typedMessage) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(typedMessage = newTypedMessage)
    }

  override def pretty: Pretty[this.type] =
    prettyOfClass(unnamedParam(_.message), param("signatures", _.signature))
}

object SignedProtocolMessage
    extends HasProtocolVersionedWithContextAndValidationCompanion[SignedProtocolMessage[
      SignedProtocolMessageContent
    ], HashOps] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signature: Signature,
      protocolVersion: ProtocolVersion,
  ): SignedProtocolMessage[M] =
    SignedProtocolMessage(typedMessage, signature)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  @VisibleForTesting
  def from[M <: SignedProtocolMessageContent](
      message: M,
      protocolVersion: ProtocolVersion,
      signature: Signature,
  ): SignedProtocolMessage[M] = SignedProtocolMessage(
    TypedSignedProtocolMessageContent(message, protocolVersion),
    signature,
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
      signature <- mkSignature(typedMessage, cryptoApi, protocolVersion)
    } yield SignedProtocolMessage(typedMessage, signature, protocolVersion)
  }

  private def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature] =
    mkSignature(typedMessage, cryptoApi, protocolVersionRepresentativeFor(protocolVersion))

  @VisibleForTesting
  private[canton] def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      protocolVersion: RepresentativeProtocolVersion[SignedProtocolMessage.type],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature] = {
    val (hashPurpose, serialization) =
      if (protocolVersion == protocolVersionRepresentativeFor(ProtocolVersion.v3)) {
        (typedMessage.content.hashPurpose, typedMessage.content.getCryptographicEvidence)
      } else {
        (HashPurpose.SignedProtocolMessageSignature, typedMessage.getCryptographicEvidence)
      }
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

  private[messages] def fromProtoV0(
      context: (HashOps, ProtocolVersion),
      signedMessageP: v0.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val (_, expectedProtocolVersion) = context
    import v0.SignedProtocolMessage.SomeSignedProtocolMessage as Sm
    val v0.SignedProtocolMessage(maybeSignatureP, messageBytes) = signedMessageP
    for {
      message <- (messageBytes match {
        case Sm.MediatorResponse(mediatorResponseBytes) =>
          MediatorResponse.fromByteString(expectedProtocolVersion)(mediatorResponseBytes)
        case Sm.TransactionResult(transactionResultMessageBytes) =>
          TransactionResultMessage.fromByteString(expectedProtocolVersion)(context)(
            transactionResultMessageBytes
          )
        case Sm.TransferResult(transferResultBytes) =>
          // No validation because the TransferResult had a deserialization bug which would deserialize ProtoV1 with
          // ProtoVersion(0) instead of ProtoVersion(1); which then also resulted in data dumps that fail the
          // deserialization validation.
          TransferResult.fromByteStringUnsafe(transferResultBytes)
        case Sm.AcsCommitment(acsCommitmentBytes) =>
          AcsCommitment.fromByteString(expectedProtocolVersion)(acsCommitmentBytes)
        case Sm.MalformedMediatorRequestResult(malformedMediatorRequestResultBytes) =>
          MalformedMediatorRequestResult.fromByteString(expectedProtocolVersion)(
            malformedMediatorRequestResultBytes
          )
        case Sm.Empty =>
          Left(OtherError("Deserialization of a SignedMessage failed due to a missing message"))
      }): ParsingResult[SignedProtocolMessageContent]
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", maybeSignatureP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
      signedMessage = SignedProtocolMessage(
        TypedSignedProtocolMessageContent(
          message,
          ProtocolVersion.minimum,
        ),
        signature,
      )(rpv)
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
