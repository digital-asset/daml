// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.digitalasset.daml.lf.data.Ref

/** Minimal, self-contained SDK-side replacements for the canton ledger API user-management domain
  * types. These mirror the shapes of `com.digitalasset.canton.ledger.api.{User, UserRight,
  * ObjectMeta, IdentityProviderId}` but carry no dependency on canton's ledger-api-core.
  */
sealed trait IdentityProviderId {
  def toRequestString: String
}

object IdentityProviderId {
  final case object Default extends IdentityProviderId {
    override def toRequestString: String = ""
  }

  final case class Id(value: Ref.LedgerString) extends IdentityProviderId {
    override def toRequestString: String = value
  }

  object Id {
    def fromString(id: String): Either[String, IdentityProviderId.Id] =
      Ref.LedgerString.fromString(id).map(Id.apply)

    def assertFromString(id: String): Id =
      Id(Ref.LedgerString.assertFromString(id))
  }

  def apply(identityProviderId: String): IdentityProviderId =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Id.assertFromString(id)
      case None => Default
    }

  def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Id.fromString(id)
      case None => Right(Default)
    }
}

final case class ObjectMeta(
    resourceVersionO: Option[Long],
    annotations: Map[String, String],
)

object ObjectMeta {
  def empty: ObjectMeta = ObjectMeta(
    resourceVersionO = None,
    annotations = Map.empty,
  )
}

final case class User(
    id: Ref.UserId,
    primaryParty: Option[Ref.Party],
    isDeactivated: Boolean = false,
    metadata: ObjectMeta = ObjectMeta.empty,
    identityProviderId: IdentityProviderId = IdentityProviderId.Default,
    primaryPartyAuthentication: Boolean = false,
)

sealed abstract class UserRight extends Product with Serializable {
  def getParty: Option[Ref.Party] = None
}

sealed abstract class UserRightForParty(party: Ref.Party) extends UserRight {
  override def getParty: Option[Ref.Party] = Some(party)
}

object UserRight {
  final case object ParticipantAdmin extends UserRight

  final case object IdentityProviderAdmin extends UserRight

  final case class CanActAs(party: Ref.Party) extends UserRightForParty(party)

  final case class CanReadAs(party: Ref.Party) extends UserRightForParty(party)

  final case object CanReadAsAnyParty extends UserRight

  final case class CanExecuteAs(party: Ref.Party) extends UserRightForParty(party)

  final case object CanExecuteAsAnyParty extends UserRight
}
