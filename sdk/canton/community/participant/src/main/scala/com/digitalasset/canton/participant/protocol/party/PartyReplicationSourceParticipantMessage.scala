// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.protocol.party.PartyReplicationSourceParticipantMessage.DataOrStatus
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class PartyReplicationSourceParticipantMessage(dataOrStatus: DataOrStatus)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationSourceParticipantMessage.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationSourceParticipantMessage] {
  @transient override protected lazy val companionObj
      : PartyReplicationSourceParticipantMessage.type =
    PartyReplicationSourceParticipantMessage

  def toProtoV30: v30.PartyReplicationSourceParticipantMessage =
    v30.PartyReplicationSourceParticipantMessage(dataOrStatus.toProtoV30)
}

object PartyReplicationSourceParticipantMessage
    extends VersioningCompanion[PartyReplicationSourceParticipantMessage] {
  override val name: String = "PartyReplicationSourceParticipantMessage"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.PartyReplicationSourceParticipantMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      proto: v30.PartyReplicationSourceParticipantMessage
  ): ParsingResult[PartyReplicationSourceParticipantMessage] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    dataOrStatus <- proto.dataOrStatus match {
      case v30.PartyReplicationSourceParticipantMessage.DataOrStatus.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("data_or_status"))
      case v30.PartyReplicationSourceParticipantMessage.DataOrStatus.AcsBatch(batchP) =>
        for {
          contracts <- batchP.contracts.toList.traverse(ActiveContract.fromProtoV30)
          nonEmptyContracts <- NonEmpty
            .from(contracts)
            .toRight(
              ProtoDeserializationError
                .ValueConversionError("contracts", "Contracts must not be empty")
            )
        } yield AcsBatch(nonEmptyContracts)
      case v30.PartyReplicationSourceParticipantMessage.DataOrStatus
            .EndOfAcs(v30.PartyReplicationSourceParticipantMessage.EndOfAcs()) =>
        Right(EndOfACS)
    }
  } yield PartyReplicationSourceParticipantMessage(dataOrStatus)(rpv)

  sealed trait DataOrStatus {
    def toProtoV30: v30.PartyReplicationSourceParticipantMessage.DataOrStatus
  }

  final case class AcsBatch(contracts: NonEmpty[Seq[ActiveContract]]) extends DataOrStatus {
    override def toProtoV30: v30.PartyReplicationSourceParticipantMessage.DataOrStatus =
      v30.PartyReplicationSourceParticipantMessage.DataOrStatus.AcsBatch(
        v30.PartyReplicationSourceParticipantMessage
          .AcsBatch(contracts.forgetNE.map(_.toProtoV30))
      )
  }

  object EndOfACS extends DataOrStatus {
    override def toProtoV30: v30.PartyReplicationSourceParticipantMessage.DataOrStatus =
      v30.PartyReplicationSourceParticipantMessage.DataOrStatus.EndOfAcs(
        v30.PartyReplicationSourceParticipantMessage.EndOfAcs()
      )
  }

  def apply(
      dataOrStatus: DataOrStatus,
      protocolVersion: ProtocolVersion,
  ): PartyReplicationSourceParticipantMessage =
    PartyReplicationSourceParticipantMessage(dataOrStatus)(
      protocolVersionRepresentativeFor(protocolVersion)
    )
}
