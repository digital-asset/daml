// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.admin.user_management_service.Right.Kind
import com.daml.ledger.api.v1.admin.user_management_service.{
  ListUsersResponse as ProtoListUsersResponse,
  Right as ProtoUserRight,
  User as ProtoLedgerApiUser,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

import scala.util.control.NoStackTrace

final case class LedgerApiUser(
    id: String,
    primaryParty: Option[PartyId],
    isDeactivated: Boolean,
    metadata: LedgerApiObjectMeta,
    identityProviderId: String,
)

object LedgerApiUser {
  def fromProtoV0(
      value: ProtoLedgerApiUser
  ): ParsingResult[LedgerApiUser] = {
    val ProtoLedgerApiUser(id, primaryParty, isDeactivated, metadataO, identityProviderId) = value
    Option
      .when(primaryParty.nonEmpty)(primaryParty)
      .traverse(LfPartyId.fromString(_).flatMap(PartyId.fromLfParty))
      .leftMap { err =>
        ProtoDeserializationError.ValueConversionError("primaryParty", err)
      }
      .map { primaryPartyO =>
        LedgerApiUser(
          id = id,
          primaryParty = primaryPartyO,
          isDeactivated = isDeactivated,
          metadata = LedgerApiObjectMeta(
            resourceVersion = metadataO.fold("")(_.resourceVersion),
            annotations = metadataO.fold(Map.empty[String, String])(_.annotations),
          ),
          identityProviderId = identityProviderId,
        )
      }
  }
}

final case class UserRights(
    actAs: Set[PartyId],
    readAs: Set[PartyId],
    participantAdmin: Boolean,
    identityProviderAdmin: Boolean,
)
object UserRights {
  def fromProtoV0(
      values: Seq[ProtoUserRight]
  ): ParsingResult[UserRights] = {
    Right(values.map(_.kind).foldLeft(UserRights(Set(), Set(), false, false)) {
      case (acc, Kind.Empty) => acc
      case (acc, Kind.ParticipantAdmin(value)) => acc.copy(participantAdmin = true)
      case (acc, Kind.CanActAs(value)) =>
        acc.copy(actAs = acc.actAs + PartyId.tryFromProtoPrimitive(value.party))
      case (acc, Kind.CanReadAs(value)) =>
        acc.copy(readAs = acc.readAs + PartyId.tryFromProtoPrimitive(value.party))
      case (acc, Kind.IdentityProviderAdmin(value)) =>
        acc.copy(identityProviderAdmin = true)
    })
  }
}

final case class ListLedgerApiUsersResult(users: Seq[LedgerApiUser], nextPageToken: String)

object ListLedgerApiUsersResult {
  def fromProtoV0(
      value: ProtoListUsersResponse,
      filterUser: String,
  ): ParsingResult[ListLedgerApiUsersResult] = {
    val ProtoListUsersResponse(protoUsers, nextPageToken) = value
    protoUsers.traverse(LedgerApiUser.fromProtoV0).map { users =>
      ListLedgerApiUsersResult(users.filter(_.id.startsWith(filterUser)), nextPageToken)
    }
  }
}

/** Represents a user value exposed in the Canton console
  */
final case class User(
    id: String,
    primaryParty: Option[PartyId],
    isActive: Boolean,
    annotations: Map[String, String],
    identityProviderId: String,
)

object User {
  def fromLapiUser(u: LedgerApiUser): User = User(
    id = u.id,
    primaryParty = u.primaryParty,
    isActive = !u.isDeactivated,
    annotations = u.metadata.annotations,
    identityProviderId = u.identityProviderId,
  )
  def toLapiUser(u: User, resourceVersion: Option[String]): LedgerApiUser = LedgerApiUser(
    id = u.id,
    primaryParty = u.primaryParty,
    isDeactivated = !u.isActive,
    metadata = LedgerApiObjectMeta(
      resourceVersion = resourceVersion.getOrElse(""),
      annotations = u.annotations,
    ),
    identityProviderId = u.identityProviderId,
  )
}

final case class ModifyingNonModifiableUserPropertiesError()
    extends RuntimeException("MODIFYING_AN_UNMODIFIABLE_USER_PROPERTY_ERROR")
    with NoStackTrace

final case class UsersPage(users: Seq[User], nextPageToken: String)
