// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import com.digitalasset.canton.crypto.HashOps
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
    extends HasMemoizedProtocolVersionedWithContextAndValidationCompanion[
      TypedSignedProtocolMessageContent[SignedProtocolMessageContent],
      HashOps,
    ] {
  override def name: String = "TypedSignedProtocolMessageContent"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.v3)
  )

  def apply[M <: SignedProtocolMessageContent](
      content: M,
      protocolVersion: ProtocolVersion,
  ): TypedSignedProtocolMessageContent[M] =
    TypedSignedProtocolMessageContent(content)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )
}
