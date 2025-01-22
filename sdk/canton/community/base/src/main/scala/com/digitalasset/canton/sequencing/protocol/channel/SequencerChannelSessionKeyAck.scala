// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class SequencerChannelSessionKeyAck()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelSessionKeyAck.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelSessionKeyAck] {
  @transient override protected lazy val companionObj: SequencerChannelSessionKeyAck.type =
    SequencerChannelSessionKeyAck

  def toProtoV30: v30.SequencerChannelSessionKeyAck =
    v30.SequencerChannelSessionKeyAck()
}

object SequencerChannelSessionKeyAck
    extends HasProtocolVersionedCompanion[SequencerChannelSessionKeyAck] {
  override val name: String = "SequencerChannelSessionKeyAck"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.SequencerChannelSessionKeyAck
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      protocolVersion: ProtocolVersion
  ): SequencerChannelSessionKeyAck =
    SequencerChannelSessionKeyAck()(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      SequencerChannelSessionKeyAcknowledgmentP: v30.SequencerChannelSessionKeyAck
  ): ParsingResult[SequencerChannelSessionKeyAck] = {
    val v30.SequencerChannelSessionKeyAck() = SequencerChannelSessionKeyAcknowledgmentP
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerChannelSessionKeyAck()(rpv)
  }
}
