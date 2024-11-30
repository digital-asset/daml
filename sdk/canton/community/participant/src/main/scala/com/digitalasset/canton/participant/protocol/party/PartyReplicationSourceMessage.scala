// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.v30
import com.digitalasset.canton.participant.protocol.party.PartyReplicationSourceMessage.DataOrStatus
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class PartyReplicationSourceMessage(dataOrStatus: DataOrStatus)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationSourceMessage.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationSourceMessage] {
  @transient override protected lazy val companionObj: PartyReplicationSourceMessage.type =
    PartyReplicationSourceMessage

  def toProtoV30: v30.PartyReplicationSourceMessage =
    v30.PartyReplicationSourceMessage(dataOrStatus.toProtoV30)
}

object PartyReplicationSourceMessage
    extends HasProtocolVersionedCompanion[PartyReplicationSourceMessage] {
  override val name: String = "PartyReplicationSourceMessage"

  override val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.PartyReplicationSourceMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      proto: v30.PartyReplicationSourceMessage
  ): ParsingResult[PartyReplicationSourceMessage] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    dataOrStatus <- proto.dataOrStatus match {
      case v30.PartyReplicationSourceMessage.DataOrStatus.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("data_or_status"))
      case v30.PartyReplicationSourceMessage.DataOrStatus.AcsChunk(chunkP) =>
        for {
          chunkCounter <- ProtoConverter.parseNonNegativeInt("chunk_counter", chunkP.chunkCounter)
          contracts <- chunkP.contracts.toList.traverse(ActiveContract.fromProtoV30)
          nonEmptyContracts <- NonEmpty
            .from(contracts)
            .toRight(
              ProtoDeserializationError
                .ValueConversionError("contracts", "Contracts must not be empty")
            )
        } yield AcsChunk(chunkCounter, nonEmptyContracts)
      case v30.PartyReplicationSourceMessage.DataOrStatus.SourceParticipantIsReady(
            v30.PartyReplicationSourceMessage.SourceParticipantIsReady()
          ) =>
        Right(SourceParticipantIsReady)
      case v30.PartyReplicationSourceMessage.DataOrStatus
            .EndOfAcs(v30.PartyReplicationSourceMessage.EndOfACS()) =>
        Right(EndOfACS)
    }
  } yield PartyReplicationSourceMessage(dataOrStatus)(rpv)

  sealed trait DataOrStatus {
    def toProtoV30: v30.PartyReplicationSourceMessage.DataOrStatus
  }

  final case class AcsChunk(chunkCounter: NonNegativeInt, contracts: NonEmpty[Seq[ActiveContract]])
      extends DataOrStatus {
    override def toProtoV30: v30.PartyReplicationSourceMessage.DataOrStatus =
      v30.PartyReplicationSourceMessage.DataOrStatus.AcsChunk(
        v30.PartyReplicationSourceMessage
          .AcsChunk(chunkCounter.value, contracts.forgetNE.map(_.toProtoV30))
      )
  }

  object SourceParticipantIsReady extends DataOrStatus {
    override def toProtoV30: v30.PartyReplicationSourceMessage.DataOrStatus =
      v30.PartyReplicationSourceMessage.DataOrStatus.SourceParticipantIsReady(
        v30.PartyReplicationSourceMessage.SourceParticipantIsReady()
      )
  }

  object EndOfACS extends DataOrStatus {
    override def toProtoV30: v30.PartyReplicationSourceMessage.DataOrStatus =
      v30.PartyReplicationSourceMessage.DataOrStatus.EndOfAcs(
        v30.PartyReplicationSourceMessage.EndOfACS()
      )
  }
}
