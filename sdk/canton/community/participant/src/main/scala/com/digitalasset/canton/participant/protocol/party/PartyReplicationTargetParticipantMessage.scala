// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class PartyReplicationTargetParticipantMessage(
    instruction: PartyReplicationTargetParticipantMessage.Instruction
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationTargetParticipantMessage.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationTargetParticipantMessage] {
  @transient override protected lazy val companionObj
      : PartyReplicationTargetParticipantMessage.type = PartyReplicationTargetParticipantMessage

  def toProtoV30: v30.PartyReplicationTargetParticipantMessage =
    v30.PartyReplicationTargetParticipantMessage(instruction.toProtoV30)
}

object PartyReplicationTargetParticipantMessage
    extends VersioningCompanion[PartyReplicationTargetParticipantMessage] {

  override val name: String = "PartyReplicationTargetParticipantMessage"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.PartyReplicationTargetParticipantMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      proto: v30.PartyReplicationTargetParticipantMessage
  ): ParsingResult[PartyReplicationTargetParticipantMessage] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    instruction <- proto.instruction match {
      case v30.PartyReplicationTargetParticipantMessage.Instruction.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("instruction"))
      case v30.PartyReplicationTargetParticipantMessage.Instruction.Initialize(
            v30.PartyReplicationTargetParticipantMessage.Initialize(
              initialContractOrdinalInclusive
            )
          ) =>
        ProtoConverter
          .parseNonNegativeInt(
            "initial_contract_ordinal_inclusive",
            initialContractOrdinalInclusive,
          )
          .map(Initialize.apply)
      case v30.PartyReplicationTargetParticipantMessage.Instruction.SendAcsUpTo(
            v30.PartyReplicationTargetParticipantMessage.SendAcsUpTo(
              maxContractOrdinalInclusive
            )
          ) =>
        ProtoConverter
          .parseNonNegativeInt(
            "max_contract_ordinal_inclusive",
            maxContractOrdinalInclusive,
          )
          .map[Instruction](SendAcsUpTo.apply)
    }
  } yield PartyReplicationTargetParticipantMessage(instruction)(rpv)

  sealed trait Instruction {
    def toProtoV30: v30.PartyReplicationTargetParticipantMessage.Instruction
  }

  final case class Initialize(initialContractOrdinalInclusive: NonNegativeInt) extends Instruction {
    override def toProtoV30: v30.PartyReplicationTargetParticipantMessage.Instruction =
      v30.PartyReplicationTargetParticipantMessage.Instruction.Initialize(
        v30.PartyReplicationTargetParticipantMessage
          .Initialize(initialContractOrdinalInclusive.value)
      )
  }

  final case class SendAcsUpTo(maxContractOrdinalInclusive: NonNegativeInt) extends Instruction {
    override def toProtoV30: v30.PartyReplicationTargetParticipantMessage.Instruction =
      v30.PartyReplicationTargetParticipantMessage.Instruction.SendAcsUpTo(
        v30.PartyReplicationTargetParticipantMessage
          .SendAcsUpTo(maxContractOrdinalInclusive.value)
      )
  }

  def apply(
      instruction: PartyReplicationTargetParticipantMessage.Instruction,
      protocolVersion: ProtocolVersion,
  ): PartyReplicationTargetParticipantMessage =
    PartyReplicationTargetParticipantMessage(instruction)(
      protocolVersionRepresentativeFor(protocolVersion)
    )
}
