// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  OriginalByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionMemoization2,
}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** @param timestampOfSigningKey The timestamp of the topology snapshot that was used for signing the content.
  *                              [[scala.None$]] if the signing timestamp can be derived from the content.
  * @param signatures            Signatures of the content provided by the different sequencers.
  */
final case class SignedContent[+A <: HasCryptographicEvidence] private (
    content: A,
    signatures: NonEmpty[Seq[Signature]],
    timestampOfSigningKey: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[SignedContent[HasCryptographicEvidence]]
    with ProtocolVersionedMemoizedEvidence
    with Serializable
    with Product {

  @transient override protected lazy val companionObj: SignedContent.type = SignedContent

  def toProtoV30: v30.SignedContent =
    v30.SignedContent(
      Some(content.getCryptographicEvidence),
      signatures.map(_.toProtoV30),
      timestampOfSigningKey.map(_.toProtoPrimitive),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], B <: HasCryptographicEvidence](
      f: A => F[B]
  )(implicit F: Functor[F]): F[SignedContent[B]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[SignedContent[B]]
      else this.copy(content = newContent)
    }

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
      purpose: HashPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = SignedContent.hashContent(snapshot.pureCrypto, content, purpose)
    snapshot.verifySignature(hash, member, signature, SigningKeyUsage.ProtocolOnly)
  }

  def deserializeContent[B <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[B]
  ): ParsingResult[SignedContent[B]] =
    this.traverse(content => contentDeserializer(content.getCryptographicEvidence))

  // TODO(i12076): Start using multiple signatures
  val signature: Signature = signatures.last1

  def copy[B <: HasCryptographicEvidence](
      content: B = this.content,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
      timestampOfSigningKey: Option[CantonTimestamp] = this.timestampOfSigningKey,
  ): SignedContent[B] =
    SignedContent.create(
      content,
      signatures,
      timestampOfSigningKey,
      representativeProtocolVersion,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object SignedContent
    extends VersioningCompanionMemoization2[
      SignedContent[HasCryptographicEvidence],
      SignedContent[BytestringWithCryptographicEvidence],
    ] {

  override def name: String = "SignedContent"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.SignedContent)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  // TODO(i12076): Start using multiple signatures
  def apply[A <: HasCryptographicEvidence](
      content: A,
      signature: Signature,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SignedContent[A] = checked( // There is only a single signature
    SignedContent.create(
      content,
      NonEmpty(Seq, signature),
      timestampOfSigningKey,
      protocolVersionRepresentativeFor(protocolVersion),
    )
  )

  def apply[A <: HasCryptographicEvidence](
      content: A,
      signatures: NonEmpty[Seq[Signature]],
      timestampOfSigningKey: Option[CantonTimestamp],
      protoVersion: ProtoVersion,
  ): ParsingResult[SignedContent[A]] =
    protocolVersionRepresentativeFor(protoVersion).map { rpv =>
      checked( // There is only a single signature
        SignedContent.create(
          content,
          signatures,
          timestampOfSigningKey,
          rpv,
        )
      )
    }

  /** Creates a new signed content from scratch. Use when creating without having an existing serialized SignedContent.
    */
  def create[A <: HasCryptographicEvidence](
      content: A,
      signatures: NonEmpty[Seq[Signature]],
      timestampOfSigningKey: Option[CantonTimestamp],
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
  ): SignedContent[A] =
    new SignedContent(content, signatures, timestampOfSigningKey)(
      representativeProtocolVersion,
      None,
    )

  private def createFromProto[A <: HasCryptographicEvidence](
      content: A,
      signatures: NonEmpty[Seq[Signature]],
      timestampOfSigningKey: Option[CantonTimestamp],
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
      originalByteString: OriginalByteString,
  ): SignedContent[A] =
    new SignedContent(content, signatures, timestampOfSigningKey)(
      representativeProtocolVersion,
      Some(originalByteString),
    )

  def create[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignedContent[A]] = {
    // as deliverEvent implements MemoizedEvidence repeated calls to serialize will return the same bytes
    // so fine to call once for the hash here and then again when serializing to protobuf
    val hash = hashContent(cryptoApi, content, purpose)
    cryptoPrivateApi
      .sign(hash, SigningKeyUsage.ProtocolOnly)
      .map(signature => SignedContent(content, signature, timestampOfSigningKey, protocolVersion))
  }

  def hashContent(
      hashOps: HashOps,
      content: HasCryptographicEvidence,
      purpose: HashPurpose,
  ): Hash =
    hashOps.digest(purpose, content.getCryptographicEvidence)

  def tryCreate[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[SignedContent[A]] =
    create(
      cryptoApi,
      cryptoPrivateApi,
      content,
      timestampOfSigningKey,
      purpose,
      protocolVersion,
    )
      .valueOr(err => throw new IllegalStateException(s"Failed to create signed content: $err"))

  def fromProtoV30(
      signedValueP: v30.SignedContent
  )(
      bytes: ByteString
  ): ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] = {
    val v30.SignedContent(content, signatures, timestampOfSigningKey) = signedValueP
    for {
      contentB <- ProtoConverter.required("content", content)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signature",
        signatures,
      )
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      signedContent = createFromProto(
        BytestringWithCryptographicEvidence(contentB),
        signatures,
        ts,
        rpv,
        bytes,
      )
    } yield signedContent
  }

  implicit def prettySignedContent[A <: HasCryptographicEvidence](implicit
      prettyA: Pretty[A]
  ): Pretty[SignedContent[A]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      unnamedParam(_.content),
      param("signatures", _.signatures),
      paramIfDefined("timestamp of signing key", _.timestampOfSigningKey),
    )
  }

  def openEnvelopes(event: SignedContent[SequencedEvent[ClosedEnvelope]])(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): WithOpeningErrors[SignedContent[SequencedEvent[DefaultOpenEnvelope]]] = {
    val (openSequencedEvent, openingErrors) =
      SequencedEvent.openEnvelopes(event.content)(protocolVersion, hashOps)
    WithOpeningErrors(
      // The signature is still valid
      event.copy(content = openSequencedEvent),
      openingErrors,
    )
  }

}
