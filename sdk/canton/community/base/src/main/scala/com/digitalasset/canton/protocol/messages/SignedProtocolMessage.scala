// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.InvariantViolation
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
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered

/** In protocol versions prior to [[com.digitalasset.canton.version.ProtocolVersion.CNTestNet]],
  * the `signatures` field contains a single signature over the `typeMessage`'s
  * [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent.content]].
  * From [[com.digitalasset.canton.version.ProtocolVersion.CNTestNet]] on, there can be any number of signatures
  * and each signature covers the serialization of the `typedMessage` itself rather than just its
  * [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent.content]], and
  * every signature needs to be valid.
  */
// TODO(#15358) Adapt comment. Most of it can be deleted.
// sealed because this class is mocked in tests
sealed case class SignedProtocolMessage[+M <: SignedProtocolMessageContent] private (
    typedMessage: TypedSignedProtocolMessageContent[M],
    signatures: NonEmpty[Seq[Signature]],
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
  ): EitherT[Future, SignatureCheckError, Unit] =
    if (
      representativeProtocolVersion >=
        companionObj.protocolVersionRepresentativeFor(ProtocolVersion.CNTestNet)
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
        companionObj.protocolVersionRepresentativeFor(ProtocolVersion.CNTestNet)
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
    SignedProtocolMessage
      .create(typedMessage, signatures, representativeProtocolVersion)
      .valueOr(err => throw new IllegalArgumentException(err.message))

  override def domainId: DomainId = message.domainId

  protected def toProtoV0: v0.SignedProtocolMessage = {
    val content = typedMessage.content.toProtoSomeSignedProtocolMessage
    v0.SignedProtocolMessage(
      signature = signatures.head1.toProtoV0.some,
      someSignedProtocolMessage = content,
    )
  }

  protected def toProtoV1: v1.SignedProtocolMessage = {
    v1.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV0),
      typedSignedProtocolMessageContent = typedMessage.toByteString,
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
    extends HasProtocolVersionedWithContextCompanion[SignedProtocolMessage[
      SignedProtocolMessageContent
    ], HashOps] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.CNTestNet
    )(v1.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  private[messages] val multipleSignaturesSupportedSince = protocolVersionRepresentativeFor(
    ProtoVersion(1)
  )

  def create[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedProtocolMessage.type],
  ): Either[InvariantViolation, SignedProtocolMessage[M]] =
    Either.cond(
      representativeProtocolVersion >= multipleSignaturesSupportedSince ||
        signatures.sizeCompare(1) == 0,
      new SignedProtocolMessage(typedMessage, signatures)(representativeProtocolVersion),
      InvariantViolation(
        s"SignedProtocolMessage supports only a single signatures in protocol versions below ${multipleSignaturesSupportedSince.representative}. Got ${signatures.size} signatures)"
      ),
    )

  def create[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): Either[InvariantViolation, SignedProtocolMessage[M]] =
    create(typedMessage, signatures, protocolVersionRepresentativeFor(protocolVersion))

  def tryCreate[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): SignedProtocolMessage[M] =
    create(typedMessage, signatures, protocolVersion).valueOr(err =>
      throw new IllegalArgumentException(err.message)
    )

  @VisibleForTesting
  def tryFrom[M <: SignedProtocolMessageContent](
      message: M,
      protocolVersion: ProtocolVersion,
      signature: Signature,
      moreSignatures: Signature*
  ): SignedProtocolMessage[M] = SignedProtocolMessage.tryCreate(
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
      signature <- mkSignature(typedMessage, cryptoApi, protocolVersion)
    } yield SignedProtocolMessage.tryCreate(typedMessage, NonEmpty(Seq, signature), protocolVersion)
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
      if (protocolVersion == protocolVersionRepresentativeFor(ProtoVersion(0))) {
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
      hashOps: HashOps,
      signedMessageP: v0.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    import v0.SignedProtocolMessage.{SomeSignedProtocolMessage as Sm}
    val v0.SignedProtocolMessage(maybeSignatureP, messageBytes) = signedMessageP
    for {
      message <- (messageBytes match {
        case Sm.MediatorResponse(mediatorResponseBytes) =>
          MediatorResponse.fromByteString(mediatorResponseBytes)
        case Sm.TransactionResult(transactionResultMessageBytes) =>
          TransactionResultMessage.fromByteString(hashOps)(transactionResultMessageBytes)
        case Sm.TransferResult(transferResultBytes) =>
          TransferResult.fromByteString(transferResultBytes)
        case Sm.AcsCommitment(acsCommitmentBytes) =>
          AcsCommitment.fromByteString(acsCommitmentBytes)
        case Sm.MalformedMediatorRequestResult(malformedMediatorRequestResultBytes) =>
          MalformedMediatorRequestResult.fromByteString(malformedMediatorRequestResultBytes)
        case Sm.Empty =>
          Left(OtherError("Deserialization of a SignedMessage failed due to a missing message"))
      }): ParsingResult[SignedProtocolMessageContent]
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", maybeSignatureP)
      signedMessage <- create(
        TypedSignedProtocolMessageContent(message, ProtoVersion(-1)),
        NonEmpty(Seq, signature),
        protocolVersionRepresentativeFor(ProtoVersion(0)),
      ).leftMap(ProtoDeserializationError.InvariantViolation.toProtoDeserializationError)
    } yield signedMessage
  }

  private def fromProtoV1(
      hashOps: HashOps,
      signedMessageP: v1.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v1.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP
    for {
      typedMessage <- TypedSignedProtocolMessageContent.fromByteString(hashOps)(typedMessageBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV0,
        "signatures",
        signaturesP,
      )
      signedMessage <- create(
        typedMessage,
        signatures,
        protocolVersionRepresentativeFor(ProtoVersion(1)),
      ).leftMap(ProtoDeserializationError.InvariantViolation.toProtoDeserializationError)
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
