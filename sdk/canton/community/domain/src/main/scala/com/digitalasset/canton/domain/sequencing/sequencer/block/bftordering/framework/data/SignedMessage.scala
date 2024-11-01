// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SequencerId

final case class SignedMessage[+MessageT <: PbftNetworkMessage](
    message: MessageT,
    from: SequencerId,
    signature: Signature,
) {
  // TODO(#21169) v1.ConsensusMessage will be ByteString in future
  def toProto: v1.SignedMessage =
    v1.SignedMessage.of(
      Some(message.toProto),
      from.toProtoPrimitive,
      signature = Some(signature.toProtoV30),
    )
}

object SignedMessage {
  def fromProto[MessageT <: PbftNetworkMessage](
      parse: v1.ConsensusMessage => ParsingResult[MessageT]
  )(
      proto: v1.SignedMessage
  ): ParsingResult[SignedMessage[MessageT]] =
    fromProtoWithSequencerId(_ => parse)(proto)

  def fromProtoWithSequencerId[A <: PbftNetworkMessage](
      parse: SequencerId => v1.ConsensusMessage => ParsingResult[A]
  )(proto: v1.SignedMessage): ParsingResult[SignedMessage[A]] = for {
    protoMessage <- proto.message.toRight(ProtoDeserializationError.FieldNotSet("message"))
    from <- SequencerId.fromProtoPrimitive(proto.from, "from")
    message <- parse(from)(protoMessage)
    signature <- Signature.fromProtoV30(proto.getSignature)
  } yield SignedMessage(message, from, signature)

  implicit def ordering[A <: PbftNetworkMessage](implicit
      ordering: Ordering[A]
  ): Ordering[SignedMessage[A]] =
    ordering.on(_.message)
}
