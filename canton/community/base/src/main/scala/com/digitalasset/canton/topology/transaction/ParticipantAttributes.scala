// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.ProtoDeserializationError.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** If [[trustLevel]] is [[TrustLevel.Vip]],
  * then [[permission]]`.`[[ParticipantPermission.canConfirm canConfirm]] must hold.
  */
final case class ParticipantAttributes(permission: ParticipantPermission, trustLevel: TrustLevel) {
  // Make sure that VIPs can always confirm so that
  // downstream code does not have to handle VIPs that cannot confirm.
  require(
    trustLevel != TrustLevel.Vip || permission.canConfirm,
    "Found a Vip that cannot confirm. This is not supported.",
  )

  def merge(elem: ParticipantAttributes): ParticipantAttributes =
    ParticipantAttributes(
      permission = ParticipantPermission.lowerOf(permission, elem.permission),
      trustLevel = TrustLevel.lowerOf(trustLevel, elem.trustLevel),
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
  def toProtoEnum: v0.ParticipantPermission

  def tryToX: ParticipantPermissionX = this match {
    case ParticipantPermission.Submission => ParticipantPermissionX.Submission
    case ParticipantPermission.Confirmation => ParticipantPermissionX.Confirmation
    case ParticipantPermission.Observation => ParticipantPermissionX.Observation
    case ParticipantPermission.Disabled =>
      throw new RuntimeException(
        "ParticipantPermission.Disable does not exist in ParticipantPermissionX"
      )
  }
}

object ParticipantPermission {
  case object Submission extends ParticipantPermission {
    override val canConfirm = true
    val level = 1
    val toProtoEnum: v0.ParticipantPermission = v0.ParticipantPermission.Submission
  }
  case object Confirmation extends ParticipantPermission {
    override val canConfirm = true
    val level = 2
    val toProtoEnum: v0.ParticipantPermission = v0.ParticipantPermission.Confirmation
  }
  case object Observation extends ParticipantPermission {
    val level = 3
    val toProtoEnum: v0.ParticipantPermission = v0.ParticipantPermission.Observation
  }
  // in 3.0, participants can't be disabled anymore. they can be purged for good
  @Deprecated(since = "3.0.0")
  case object Disabled extends ParticipantPermission {
    override def isActive = false
    val level = 4
    val toProtoEnum = v0.ParticipantPermission.Disabled
  }
  // TODO(i2213): add purging of participants

  def fromProtoEnum(
      permission: v0.ParticipantPermission
  ): ParsingResult[ParticipantPermission] = {
    permission match {
      case v0.ParticipantPermission.Observation => Right(ParticipantPermission.Observation)
      case v0.ParticipantPermission.Confirmation => Right(ParticipantPermission.Confirmation)
      case v0.ParticipantPermission.Submission => Right(ParticipantPermission.Submission)
      case v0.ParticipantPermission.Disabled => Right(ParticipantPermission.Disabled)
      case v0.ParticipantPermission.MissingParticipantPermission =>
        Left(FieldNotSet(permission.name))
      case v0.ParticipantPermission.Unrecognized(x) => Left(UnrecognizedEnum(permission.name, x))
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

/** The trust level of the participant. Can be either Ordinary or Vip
  */
sealed trait TrustLevel extends Product with Serializable with PrettyPrinting {
  def toProtoEnum: v0.TrustLevel
  def rank: Byte

  override def pretty: Pretty[TrustLevel] = prettyOfObject[TrustLevel]

  def toX: TrustLevelX = this match {
    case TrustLevel.Ordinary => TrustLevelX.Ordinary
    case TrustLevel.Vip => TrustLevelX.Vip
  }
}

object TrustLevel {

  def lowerOf(fst: TrustLevel, snd: TrustLevel): TrustLevel = if (fst.rank < snd.rank) fst else snd

  def higherOf(fst: TrustLevel, snd: TrustLevel): TrustLevel = if (fst.rank > snd.rank) fst else snd

  case object Ordinary extends TrustLevel {
    override def toProtoEnum: v0.TrustLevel = v0.TrustLevel.Ordinary
    override def rank: Byte = 0;
  }
  case object Vip extends TrustLevel {
    override def toProtoEnum: v0.TrustLevel = v0.TrustLevel.Vip
    override def rank: Byte = 1;
  }

  def fromProtoEnum(value: v0.TrustLevel): ParsingResult[TrustLevel] =
    value match {
      case v0.TrustLevel.Vip => Right(Vip)
      case v0.TrustLevel.Ordinary => Right(Ordinary)
      case v0.TrustLevel.MissingTrustLevel => Left(FieldNotSet("trustLevel"))
      case v0.TrustLevel.Unrecognized(x) => Left(UnrecognizedEnum("trustLevel", x))
    }

  implicit val orderingTrustLevel: Ordering[TrustLevel] = Ordering.by[TrustLevel, Byte](_.rank)
}
