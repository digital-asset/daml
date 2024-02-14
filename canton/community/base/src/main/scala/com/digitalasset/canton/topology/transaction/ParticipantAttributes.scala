// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.order.*
import com.digitalasset.canton.ProtoDeserializationError.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class ParticipantAttributes(
    permission: ParticipantPermission,
    loginAfter: Option[CantonTimestamp] = None,
) {

  def merge(elem: ParticipantAttributes): ParticipantAttributes =
    ParticipantAttributes(
      permission = ParticipantPermission.lowerOf(permission, elem.permission),
      loginAfter = loginAfter.max(elem.loginAfter),
    )

}

/** Permissions of a participant, i.e., things a participant can do on behalf of a party
  *
  * Permissions are hierarchical. A participant who can submit can confirm. A participant who can confirm can observe.
  */
sealed trait ParticipantPermission extends Product with Serializable {
  def canConfirm: Boolean = false // can confirm transactions
  def isActive: Boolean = true // can receive messages
  val level: Byte // used for serialization and ordering.
  def toProtoEnum: v30.Enums.ParticipantPermission

  def tryToX: ParticipantPermissionX = this match {
    case ParticipantPermission.Submission => ParticipantPermissionX.Submission
    case ParticipantPermission.Confirmation => ParticipantPermissionX.Confirmation
    case ParticipantPermission.Observation => ParticipantPermissionX.Observation
  }
}

object ParticipantPermission {
  case object Submission extends ParticipantPermission {
    override val canConfirm = true
    val level = 1
    val toProtoEnum: v30.Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
  }
  case object Confirmation extends ParticipantPermission {
    override val canConfirm = true
    val level = 2
    val toProtoEnum: v30.Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
  }
  case object Observation extends ParticipantPermission {
    val level = 3
    val toProtoEnum: v30.Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
  }
  // TODO(i2213): add purging of participants

  def fromProtoEnum(
      permission: v30.Enums.ParticipantPermission
  ): ParsingResult[ParticipantPermission] = {
    permission match {
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
        Right(ParticipantPermission.Observation)
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
        Right(ParticipantPermission.Confirmation)
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
        Right(ParticipantPermission.Submission)
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED =>
        Left(FieldNotSet(permission.name))
      case v30.Enums.ParticipantPermission.Unrecognized(x) =>
        Left(UnrecognizedEnum(permission.name, x))
    }
  }

  implicit val orderingParticipantPermission: Ordering[ParticipantPermission] =
    Ordering.by[ParticipantPermission, Byte](_.level).reverse

  def lowerOf(fst: ParticipantPermission, snd: ParticipantPermission): ParticipantPermission = {
    if (fst.level > snd.level)
      fst
    else snd
  }

  def higherOf(fst: ParticipantPermission, snd: ParticipantPermission): ParticipantPermission = {
    if (fst.level < snd.level)
      fst
    else snd
  }

}
