// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class TypedSignedProtocolMessageContent[+M <: SignedProtocolMessageContent] private (
    content: M
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TypedSignedProtocolMessageContent.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[
      TypedSignedProtocolMessageContent[SignedProtocolMessageContent]
    ]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TypedSignedProtocolMessageContent.type =
    TypedSignedProtocolMessageContent

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  private def toProtoV0: v0.TypedSignedProtocolMessageContent =
    v0.TypedSignedProtocolMessageContent(
      someSignedProtocolMessage = content.toProtoTypedSomeSignedProtocolMessage
    )

  @VisibleForTesting
  def copy[MM <: SignedProtocolMessageContent](
      content: MM = this.content
  ): TypedSignedProtocolMessageContent[MM] =
    TypedSignedProtocolMessageContent(content)(representativeProtocolVersion, None)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[messages] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[TypedSignedProtocolMessageContent[MM]] = {
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[TypedSignedProtocolMessageContent[MM]]
      else
        TypedSignedProtocolMessageContent(newContent)(
          representativeProtocolVersion,
          deserializedFrom,
        )
    }
  }
}

object TypedSignedProtocolMessageContent
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      TypedSignedProtocolMessageContent[SignedProtocolMessageContent]
    ] {
  override def name: String = "TypedSignedProtocolMessageContent"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v30
    )(v0.TypedSignedProtocolMessageContent)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply[M <: SignedProtocolMessageContent](
      content: M,
      protocolVersion: ProtocolVersion,
  ): TypedSignedProtocolMessageContent[M] =
    TypedSignedProtocolMessageContent(content)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def apply[M <: SignedProtocolMessageContent](
      content: M,
      protoVersion: ProtoVersion,
  ): TypedSignedProtocolMessageContent[M] =
    TypedSignedProtocolMessageContent(content)(protocolVersionRepresentativeFor(protoVersion), None)

  // TODO(#12626) – try with context
  private def fromProtoV0(proto: v0.TypedSignedProtocolMessageContent)(
      bytes: ByteString
  ): ParsingResult[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]] = {
    import v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage as Sm
    val v0.TypedSignedProtocolMessageContent(messageBytes) = proto
    for {
      message <- (messageBytes match {
        case Sm.MediatorResponse(mediatorResponseBytes) =>
          MediatorResponse.fromByteStringUnsafe(mediatorResponseBytes)
        case Sm.TransactionResult(transactionResultMessageBytes) =>
          TransactionResultMessage.fromByteStringUnsafe(transactionResultMessageBytes)
        case Sm.TransferResult(transferResultBytes) =>
          TransferResult.fromByteStringUnsafe(transferResultBytes)
        case Sm.AcsCommitment(acsCommitmentBytes) =>
          AcsCommitment.fromByteStringUnsafe(acsCommitmentBytes)
        case Sm.MalformedMediatorRequestResult(malformedMediatorRequestResultBytes) =>
          MalformedMediatorRequestResult.fromByteStringUnsafe(
            malformedMediatorRequestResultBytes
          )
        case Sm.Empty =>
          Left(OtherError("Deserialization of a SignedMessage failed due to a missing message"))
      }): ParsingResult[SignedProtocolMessageContent]
    } yield TypedSignedProtocolMessageContent(message)(
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
  }
}
