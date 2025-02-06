// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.protocol.v30
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

  private def toProtoV30: v30.TypedSignedProtocolMessageContent =
    v30.TypedSignedProtocolMessageContent(
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
  )(implicit F: Functor[F]): F[TypedSignedProtocolMessageContent[MM]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[TypedSignedProtocolMessageContent[MM]]
      else
        TypedSignedProtocolMessageContent(newContent)(
          representativeProtocolVersion,
          deserializedFrom,
        )
    }
}

object TypedSignedProtocolMessageContent
    extends VersioningCompanionContextMemoization[
      TypedSignedProtocolMessageContent[SignedProtocolMessageContent],
      ProtocolVersion,
    ] {
  override def name: String = "TypedSignedProtocolMessageContent"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v33
    )(v30.TypedSignedProtocolMessageContent)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
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
  ): ParsingResult[TypedSignedProtocolMessageContent[M]] = protocolVersionRepresentativeFor(
    protoVersion
  ).map(TypedSignedProtocolMessageContent(content)(_, None))

  private def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      proto: v30.TypedSignedProtocolMessageContent,
  )(
      bytes: ByteString
  ): ParsingResult[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]] = {
    import v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage as Sm
    val v30.TypedSignedProtocolMessageContent(messageBytes) = proto
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      message <- (messageBytes match {
        case Sm.ConfirmationResponse(confirmationResponseBytes) =>
          ConfirmationResponse.fromByteString(expectedProtocolVersion, confirmationResponseBytes)
        case Sm.ConfirmationResult(confirmationResultMessageBytes) =>
          ConfirmationResultMessage.fromByteString(
            expectedProtocolVersion,
            confirmationResultMessageBytes,
          )
        case Sm.AcsCommitment(acsCommitmentBytes) =>
          AcsCommitment.fromByteString(expectedProtocolVersion, acsCommitmentBytes)
        case Sm.SetTrafficPurchased(setTrafficPurchasedBytes) =>
          SetTrafficPurchasedMessage.fromByteString(
            expectedProtocolVersion,
            setTrafficPurchasedBytes,
          )
        case Sm.Empty =>
          Left(OtherError("Deserialization of a SignedMessage failed due to a missing message"))
      }): ParsingResult[SignedProtocolMessageContent]
    } yield TypedSignedProtocolMessageContent(message)(rpv, Some(bytes))
  }
}
