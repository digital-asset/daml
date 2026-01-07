// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoApi,
  SyncCryptoError,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, OpenEnvelope}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Parent trait of messages that are sent through the sequencer
  */
sealed trait ProtocolMessage
    extends Product
    with Serializable
    with HasPhysicalSynchronizerId
    with PrettyPrinting
    with HasRepresentativeProtocolVersion {

  override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type]

  /** The ID of the synchronizer over which this message is supposed to be sent. */
  def psid: PhysicalSynchronizerId

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking
    * confidential data. Sub-classes may override the pretty instance to print more information.
    */
  @VisibleForTesting
  override def pretty: Pretty[this.type] = prettyOfObject[ProtocolMessage]
}

object ProtocolMessage {

  /** Returns the envelopes from the batch that match the given synchronizer id. If any other
    * messages exist, it gives them to the provided callback
    */
  def filterSynchronizerEnvelopes[M <: ProtocolMessage](
      envelopes: Seq[OpenEnvelope[M]],
      synchronizerId: PhysicalSynchronizerId,
  )(
      onWrongSynchronizer: Seq[OpenEnvelope[M]] => Unit
  ): Seq[OpenEnvelope[M]] = {
    val (withCorrectSynchronizerId, withWrongSynchronizerId) =
      envelopes.partition(_.protocolMessage.psid == synchronizerId)
    if (withWrongSynchronizerId.nonEmpty)
      onWrongSynchronizer(withWrongSynchronizerId)
    withCorrectSynchronizerId
  }

  trait ProtocolMessageContentCast[A <: ProtocolMessage] {
    def toKind(message: ProtocolMessage): Option[A]
    def targetKind: String
  }

  object ProtocolMessageContentCast {
    def create[A <: ProtocolMessage](name: String)(
        cast: ProtocolMessage => Option[A]
    ): ProtocolMessageContentCast[A] = new ProtocolMessageContentCast[A] {
      override def toKind(message: ProtocolMessage): Option[A] = cast(message)

      override def targetKind: String = name
    }
  }

  def toKind[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[M] =
    cast.toKind(envelope.protocolMessage)

  def select[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[OpenEnvelope[M]] =
    envelope.traverse(cast.toKind)
}

/** Marker trait for [[ProtocolMessage]]s that are not a [[SignedProtocolMessage]] */
trait UnsignedProtocolMessage extends ProtocolMessage {
  protected[messages] def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent
}

/** There can be any number of signatures. Every signature covers the serialization of the
  * `typedMessage` and needs to be valid.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedProtocolMessage[+M <: SignedProtocolMessageContent](
    typedMessage: TypedSignedProtocolMessageContent[M],
    signatures: NonEmpty[Seq[Signature]],
) extends ProtocolMessage
    with HasProtocolVersionedWrapper[SignedProtocolMessage[SignedProtocolMessageContent]] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    SignedProtocolMessage.type
  ] = SignedProtocolMessage.protocolVersionRepresentativeFor(
    typedMessage.content.psid.protocolVersion
  )

  @transient override protected lazy val companionObj: SignedProtocolMessage.type =
    SignedProtocolMessage

  def message: M = typedMessage.content

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifySignatures(
      snapshot,
      member,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifyMediatorSignatures(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifyMediatorSignatures(
      snapshot,
      mediatorGroupIndex,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifySequencerSignatures(
      snapshot: SyncCryptoApi
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifySequencerSignatures(
      snapshot,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signatures)

  override def psid: PhysicalSynchronizerId = message.psid

  protected def toProtoV30: v30.SignedProtocolMessage =
    v30.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV30),
      typedSignedProtocolMessageContent = typedMessage.toByteString,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] =
    F.map(typedMessage.traverse(f)) { newTypedMessage =>
      if (newTypedMessage eq typedMessage) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(typedMessage = newTypedMessage)
    }

  override def pretty: Pretty[this.type] =
    prettyOfClass(unnamedParam(_.message), param("signatures", _.signatures))
}

object SignedProtocolMessage
    extends VersioningCompanionContext[SignedProtocolMessage[
      SignedProtocolMessageContent
    ], ProtocolVersionValidation] {
  override val name: String = "SignedProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v34
    )(v30.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  @VisibleForTesting
  def from[M <: SignedProtocolMessageContent](
      message: M,
      signature: Signature,
      moreSignatures: Signature*
  ): SignedProtocolMessage[M] = SignedProtocolMessage(
    TypedSignedProtocolMessageContent(message),
    NonEmpty(Seq, signature, moreSignatures*),
  )

  /** @param approximateTimestampOverride
    *   optional timestamp to use for signing. Should only be set when signing submission requests,
    *   encrypted view messages or any other times when the topology is not yet fixed, i.e., when
    *   using a topology snapshot approximation. The current local clock reading is often a suitable
    *   value.
    */
  def signAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      approximateTimestampOverride: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignedProtocolMessage[M]] = {
    val typedMessage = TypedSignedProtocolMessageContent(message)
    for {
      signature <- mkSignature(typedMessage, cryptoApi, approximateTimestampOverride)
    } yield SignedProtocolMessage(typedMessage, NonEmpty(Seq, signature))
  }

  /** @param approximateTimestampOverride
    *   optional timestamp to use for signing. Should only be set when signing submission requests,
    *   encrypted view messages or any other times when the topology is not yet fixed, i.e., when
    *   using a topology snapshot approximation. The current local clock reading is often a suitable
    *   value.
    */
  @VisibleForTesting
  private[canton] def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      approximateTimestampOverride: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] = {
    val hashPurpose = HashPurpose.SignedProtocolMessageSignature
    val serialization = typedMessage.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    cryptoApi.sign(hash, SigningKeyUsage.ProtocolOnly, approximateTimestampOverride)
  }

  def trySignAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      approximateTimestampOverride: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[SignedProtocolMessage[M]] =
    signAndCreate(message, cryptoApi, approximateTimestampOverride)
      .valueOr(err =>
        throw new IllegalStateException(s"Failed to create signed protocol message: $err")
      )

  private def fromProtoV30(
      expectedProtocolVersion: ProtocolVersionValidation,
      signedMessageP: v30.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v30.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP

    for {
      typedMessage <- TypedSignedProtocolMessageContent
        .fromByteStringPVV(expectedProtocolVersion, typedMessageBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      signedMessage = SignedProtocolMessage(typedMessage, signatures)
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
