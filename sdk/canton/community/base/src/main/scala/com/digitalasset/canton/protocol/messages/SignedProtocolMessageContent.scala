// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence

trait SignedProtocolMessageContent
    extends ProtocolVersionedMemoizedEvidence
    with HasDomainId
    with PrettyPrinting
    with Product
    with Serializable {

  /** Converts this object into a [[com.google.protobuf.ByteString]] using [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence.getCryptographicEvidence]]
    * and wraps the result in the appropriate [[com.digitalasset.canton.protocol.v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage]] constructor.
    */
  protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage

  /** The timestamp of the [[com.digitalasset.canton.crypto.SyncCryptoApi]] used for signing this message.
    * If no timestamp is provided, the head snapshot will be used. This is only used for security tests.
    */
  def signingTimestamp: Option[CantonTimestamp]

  override protected def pretty: Pretty[this.type] = prettyOfObject[SignedProtocolMessageContent]
}

object SignedProtocolMessageContent {
  trait SignedMessageContentCast[A] {
    def toKind(content: SignedProtocolMessageContent): Option[A]

    def targetKind: String
  }

  object SignedMessageContentCast {
    def create[A](name: String)(
        cast: SignedProtocolMessageContent => Option[A]
    ): SignedMessageContentCast[A] = new SignedMessageContentCast[A] {
      override def toKind(content: SignedProtocolMessageContent): Option[A] = cast(content)

      override def targetKind: String = name
    }
  }
}
