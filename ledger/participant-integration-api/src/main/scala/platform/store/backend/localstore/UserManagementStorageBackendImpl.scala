// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import anorm.SqlParser.{bool, int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderId, UserRight}
import com.daml.ledger.api.domain.UserRight.{
  CanActAs,
  CanReadAs,
  IdentityProviderAdmin,
  ParticipantAdmin,
}
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.backend.common.{ComposableQuery, QueryStrategy}
import com.daml.platform.{LedgerString, Party, UserId}

import java.sql.Connection
import scala.util.Try

object UserManagementStorageBackendImpl extends UserManagementStorageBackend {

  private val ParticipantUserParser
      : RowParser[(Int, String, Option[String], Option[String], Boolean, Long, Long)] = {
    import com.daml.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    int("internal_id") ~
      str("user_id") ~
      str("primary_party").? ~
      str("identity_provider_id").? ~
      bool("is_deactivated") ~
      long("resource_version") ~
      long("created_at") map {
        case internalId ~ userId ~ primaryParty ~ identityProviderId ~ isDeactivated ~ resourceVersion ~ createdAt =>
          (
            internalId,
            userId,
            primaryParty,
            identityProviderId,
            isDeactivated,
            resourceVersion,
            createdAt,
          )
      }
  }

  private val UserRightParser: RowParser[(Int, Option[String], Long)] =
    int("user_right") ~ str("for_party").? ~ long("granted_at") map {
      case userRight ~ forParty ~ grantedAt =>
        (userRight, forParty, grantedAt)
    }

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  override def createUser(
      user: UserManagementStorageBackend.DbUserPayload
  )(connection: Connection): Int = {
    val id = user.id: String
    val primaryParty = user.primaryPartyO: Option[String]
    val identityProviderId = user.identityProviderId.map(_.value): Option[String]
    val isDeactivated = user.isDeactivated
    val resourceVersion = user.resourceVersion
    val createdAt = user.createdAt
    val internalId: Try[Int] =
      SQL"""
         INSERT INTO participant_users (user_id, primary_party, identity_provider_id, is_deactivated, resource_version, created_at)
         VALUES ($id, $primaryParty, $identityProviderId, $isDeactivated, $resourceVersion, $createdAt)
       """.executeInsert1("internal_id")(SqlParser.scalar[Int].single)(connection)
    internalId.get
  }

  override def addUserAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit = {
    ParticipantMetadataBackend.addAnnotation("participant_user_annotations")(
      internalId,
      key,
      value,
      updatedAt,
    )(connection)
  }

  override def deleteUserAnnotations(internalId: Int)(connection: Connection): Unit = {
    ParticipantMetadataBackend.deleteAnnotations("participant_user_annotations")(internalId)(
      connection
    )
  }

  override def getUserAnnotations(internalId: Int)(connection: Connection): Map[String, String] = {
    ParticipantMetadataBackend.getAnnotations("participant_user_annotations")(internalId)(
      connection
    )
  }

  override def compareAndIncreaseResourceVersion(
      internalId: Int,
      expectedResourceVersion: Long,
  )(connection: Connection): Boolean = {
    ParticipantMetadataBackend.compareAndIncreaseResourceVersion("participant_users")(
      internalId,
      expectedResourceVersion,
    )(connection)
  }

  override def increaseResourceVersion(internalId: Int)(
      connection: Connection
  ): Boolean = {
    ParticipantMetadataBackend.increaseResourceVersion("participant_users")(internalId)(connection)
  }

  override def getUser(
      id: UserId
  )(connection: Connection): Option[UserManagementStorageBackend.DbUserWithId] = {
    SQL"""
       SELECT internal_id, user_id, primary_party, is_deactivated, identity_provider_id, resource_version, created_at
       FROM participant_users
       WHERE user_id = ${id: String}
       """
      .as(ParticipantUserParser.singleOpt)(connection)
      .map {
        case (
              internalId,
              userId,
              primaryPartyRaw,
              identityProviderId,
              isDeactivated,
              resourceVersion,
              createdAt,
            ) =>
          UserManagementStorageBackend.DbUserWithId(
            internalId = internalId,
            payload = UserManagementStorageBackend.DbUserPayload(
              id = UserId.assertFromString(userId),
              primaryPartyO = dbStringToPartyString(primaryPartyRaw),
              identityProviderId = dbStringToIdentityProviderId(identityProviderId),
              isDeactivated = isDeactivated,
              resourceVersion = resourceVersion,
              createdAt = createdAt,
            ),
          )
      }
  }

  override def getUsersOrderedById(
      fromExcl: Option[UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(
      connection: Connection
  ): Vector[UserManagementStorageBackend.DbUserWithId] = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val userIdWhereClause = fromExcl match {
      case None => Nil
      case Some(id: String) => List(cSQL"user_id > ${id}")
    }
    val identityProviderIdWhereClause = identityProviderId match {
      case IdentityProviderId.Default =>
        List(cSQL"identity_provider_id is NULL")
      case IdentityProviderId.Id(value) =>
        List(cSQL"identity_provider_id=${value: String}")
    }
    val whereClause = {
      val clauses = userIdWhereClause ++ identityProviderIdWhereClause
      if (clauses.nonEmpty) {
        cSQL"WHERE ${clauses.mkComposite("", " AND ", "")}"
      } else
        cSQL""
    }
    SQL"""SELECT internal_id, user_id, primary_party, identity_provider_id, is_deactivated, resource_version, created_at
          FROM participant_users
          $whereClause
          ORDER BY user_id
          ${QueryStrategy.limitClause(Some(maxResults))}"""
      .asVectorOf(ParticipantUserParser)(connection)
      .map {
        case (
              internalId,
              userId,
              primaryPartyRaw,
              identityProviderId,
              isDeactivated,
              resourceVersion,
              createdAt,
            ) =>
          UserManagementStorageBackend.DbUserWithId(
            internalId = internalId,
            payload = UserManagementStorageBackend.DbUserPayload(
              id = UserId.assertFromString(userId),
              primaryPartyO = dbStringToPartyString(primaryPartyRaw),
              identityProviderId = dbStringToIdentityProviderId(identityProviderId),
              isDeactivated = isDeactivated,
              resourceVersion = resourceVersion,
              createdAt = createdAt,
            ),
          )
      }
  }

  override def deleteUser(userId: UserId)(connection: Connection): Boolean = {
    val updatedRowsCount =
      SQL"""
         DELETE FROM participant_users WHERE user_id = ${userId: String}
         """.executeUpdate()(connection)
    updatedRowsCount == 1
  }

  override def userRightExists(internalId: Int, right: UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: Option[Party]) = fromUserRight(right)

    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
         SELECT 1 AS dummy
         FROM participant_user_rights ur
         WHERE ur.user_internal_id = ${internalId}
               AND
               ur.user_right = ${userRight}
               AND
               ur.for_party ${isForPartyPredicate(forParty)}""".asVectorOf(IntParser0)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

  override def addUserRight(internalId: Int, right: UserRight, grantedAt: Long)(
      connection: Connection
  ): Unit = {
    val (userRight: Int, forParty: Option[Party]) = fromUserRight(right)
    val _ =
      SQL"""
         INSERT INTO participant_user_rights (user_internal_id, user_right, for_party, granted_at)
         VALUES (
            ${internalId},
            ${userRight},
            ${forParty: Option[String]},
            $grantedAt
         )
         """.executeUpdate()(connection)
  }

  override def getUserRights(
      internalId: Int
  )(connection: Connection): Set[UserManagementStorageBackend.DbUserRight] = {
    val rec =
      SQL"""
         SELECT ur.user_right, ur.for_party, ur.granted_at
         FROM participant_user_rights ur
         WHERE ur.user_internal_id = ${internalId}
         """.asVectorOf(UserRightParser)(connection)
    rec.map { case (userRight, forPartyRaw, grantedAt) =>
      UserManagementStorageBackend.DbUserRight(
        makeUserRight(
          value = userRight,
          partyRaw = forPartyRaw,
        ),
        grantedAt = grantedAt,
      )
    }.toSet
  }

  override def deleteUserRight(internalId: Int, right: domain.UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: Option[Party]) = fromUserRight(right)

    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val updatedRowCount: Int =
      SQL"""
           DELETE FROM participant_user_rights ur
           WHERE
            ur.user_internal_id = ${internalId}
            AND
            ur.user_right = ${userRight}
            AND
            ur.for_party ${isForPartyPredicate(forParty)}
           """.executeUpdate()(connection)
    updatedRowCount == 1
  }

  override def countUserRights(internalId: Int)(connection: Connection): Int = {
    SQL"SELECT count(*) AS user_rights_count from participant_user_rights WHERE user_internal_id = ${internalId}"
      .as(SqlParser.int("user_rights_count").single)(connection)
  }

  private def makeUserRight(value: Int, partyRaw: Option[String]): UserRight = {
    val partyO = dbStringToPartyString(partyRaw)
    (value, partyO) match {
      case (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None) => ParticipantAdmin
      case (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party)) => CanActAs(party)
      case (Right.CAN_READ_AS_FIELD_NUMBER, Some(party)) => CanReadAs(party)
      case (Right.IDENTITY_PROVIDER_ADMIN_FIELD_NUMBER, None) => IdentityProviderAdmin
      case _ =>
        throw new RuntimeException(s"Could not convert ${(value, partyO)} to a user right.")
    }
  }

  private def fromUserRight(right: UserRight): (Int, Option[Party]) = {
    right match {
      case ParticipantAdmin => (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None)
      case IdentityProviderAdmin => (Right.IDENTITY_PROVIDER_ADMIN_FIELD_NUMBER, None)
      case CanActAs(party) => (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party))
      case CanReadAs(party) => (Right.CAN_READ_AS_FIELD_NUMBER, Some(party))
      case _ =>
        throw new RuntimeException(s"Could not recognize user right: $right.")
    }
  }

  private def dbStringToPartyString(raw: Option[String]): Option[Party] = {
    raw.map(Party.assertFromString)
  }

  private def dbStringToIdentityProviderId(
      raw: Option[String]
  ): Option[IdentityProviderId.Id] =
    raw.map(LedgerString.assertFromString).map(IdentityProviderId.Id.apply)

  private def isForPartyPredicate(forParty: Option[Party]): ComposableQuery.CompositeSql = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    forParty.fold(cSQL"IS NULL") { party: Party =>
      cSQL"= ${party: String}"
    }
  }

  override def updateUserPrimaryParty(internalId: Int, primaryPartyO: Option[Party])(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE participant_users
         SET primary_party  = ${primaryPartyO: Option[String]}
         WHERE
             internal_id = ${internalId}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateUserIsDeactivated(internalId: Int, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE participant_users
         SET is_deactivated  = $isDeactivated
         WHERE
             internal_id = ${internalId}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateUserIdentityProviderId(
      internalId: Int,
      identityProviderId: Option[IdentityProviderId.Id],
  )(connection: Connection): Boolean = {
    IdentityProviderAwareBackend.updateIdentityProviderId("participant_users")(
      internalId,
      identityProviderId,
    )(
      connection
    )
  }

}
