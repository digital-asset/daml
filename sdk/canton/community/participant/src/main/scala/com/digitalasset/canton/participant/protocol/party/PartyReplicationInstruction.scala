// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.RefinedDurationConversionError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.participant.admin.party.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class PartyReplicationInstruction(maxCounter: NonNegativeInt)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationInstruction.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationInstruction] {
  @transient override protected lazy val companionObj: PartyReplicationInstruction.type =
    PartyReplicationInstruction

  def toProtoV30: v30.PartyReplicationInstruction =
    v30.PartyReplicationInstruction(maxCounter.value)
}

object PartyReplicationInstruction
    extends HasProtocolVersionedCompanion[PartyReplicationInstruction] {
  override val name: String = "PartyReplicationInstruction"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.PartyReplicationInstruction
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      instructionP: v30.PartyReplicationInstruction
  ): ParsingResult[PartyReplicationInstruction] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    chunkCounter <- NonNegativeInt
      .create(instructionP.maxCounter)
      .leftMap(e => RefinedDurationConversionError("max_counter", e.message))
  } yield PartyReplicationInstruction(chunkCounter)(rpv)
}
