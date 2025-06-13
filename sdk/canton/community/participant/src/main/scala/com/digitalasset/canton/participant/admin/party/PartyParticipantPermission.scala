// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{ProtoDeserializationError, topology}

/** Convenience methods to convert between external, internal, and Daml ParticipantPermission.
  */
object PartyParticipantPermission {

  // Conversion to proto is public for use by canton console client admin commands.
  def toProtoPrimitive(
      participantPermission: topology.transaction.ParticipantPermission
  ): v30.ParticipantPermission = participantPermission match {
    case topology.transaction.ParticipantPermission.Submission =>
      v30.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
    case topology.transaction.ParticipantPermission.Confirmation =>
      v30.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
    case topology.transaction.ParticipantPermission.Observation =>
      v30.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
  }

  private[admin] def fromProtoV30(
      proto: v30.ParticipantPermission
  ): ParsingResult[Option[topology.transaction.ParticipantPermission]] = proto match {
    case v30.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
      Right(Some(topology.transaction.ParticipantPermission.Submission))
    case v30.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
      Right(Some(topology.transaction.ParticipantPermission.Confirmation))
    case v30.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
      Right(Some(topology.transaction.ParticipantPermission.Observation))
    case v30.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED =>
      Right(None)
    case v30.ParticipantPermission.Unrecognized(unknown) =>
      Left(ProtoDeserializationError.UnrecognizedEnum(proto.name, unknown))
  }

  private[party] def fromDaml(
      damlParticipantPermission: M.partyreplication.ParticipantPermission
  ): topology.transaction.ParticipantPermission =
    damlParticipantPermission match {
      case M.partyreplication.ParticipantPermission.SUBMISSION =>
        topology.transaction.ParticipantPermission.Submission
      case M.partyreplication.ParticipantPermission.CONFIRMATION =>
        topology.transaction.ParticipantPermission.Confirmation
      case M.partyreplication.ParticipantPermission.OBSERVATION =>
        topology.transaction.ParticipantPermission.Observation
    }

  private[party] def toDaml(
      participantPermission: topology.transaction.ParticipantPermission
  ): M.partyreplication.ParticipantPermission =
    participantPermission match {
      case topology.transaction.ParticipantPermission.Submission =>
        M.partyreplication.ParticipantPermission.SUBMISSION
      case topology.transaction.ParticipantPermission.Confirmation =>
        M.partyreplication.ParticipantPermission.CONFIRMATION
      case topology.transaction.ParticipantPermission.Observation =>
        M.partyreplication.ParticipantPermission.OBSERVATION
    }
}
