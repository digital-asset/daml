// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.UserRight
import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.platform.{Party, UserId}
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.backend.UserManagementStorageBackend

import scala.util.Try

object UserManagementStorageBackendImpl extends UserManagementStorageBackend {

  private val ParticipantUserParser: RowParser[(Int, String, Option[String], Long)] =
    int("internal_id") ~ str("user_id") ~ str("primary_party").? ~ long("created_at") map {
      case internalId ~ userId ~ primaryParty ~ createdAt =>
        (internalId, userId, primaryParty, createdAt)
    }

  private val ParticipantUserParser2: RowParser[(String, Option[String])] =
    str("user_id") ~ str("primary_party").? map { case userId ~ primaryParty =>
      (userId, primaryParty)
    }

  private val UserRightParser: RowParser[(Int, Option[String], Long)] =
    int("user_right") ~ str("for_party").? ~ long("granted_at") map {
      case userRight ~ forParty ~ grantedAt =>
        (userRight, forParty, grantedAt)
    }

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  override def createUser(user: domain.User, createdAt: Long)(
      connection: Connection
  ): Int = {
    val internalId: Try[Int] =
      SQL"""
         INSERT INTO participant_users (user_id, primary_party, created_at)
         VALUES (${user.id: String}, ${user.primaryParty: Option[String]}, $createdAt)
       """.executeInsert1("internal_id")(SqlParser.scalar[Int].single)(connection)
    internalId.get
  }

  override def getUser(
      id: UserId
  )(connection: Connection): Option[UserManagementStorageBackend.DbUser] = {
    SQL"""
       SELECT internal_id, user_id, primary_party, created_at
       FROM participant_users
       WHERE user_id = ${id: String}
       """
      .as(ParticipantUserParser.singleOpt)(connection)
      .map { case (internalId, userId, primaryPartyRaw, createdAt) =>
        UserManagementStorageBackend.DbUser(
          internalId = internalId,
          domainUser = domain.User(
            id = UserId.assertFromString(userId),
            primaryParty = dbStringToPartyString(primaryPartyRaw),
          ),
          createdAt = createdAt,
        )
      }
  }

  override def getUsersOrderedById(fromExcl: Option[UserId], maxResults: Int)(
      connection: Connection
  ): Vector[domain.User] = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val whereClause = fromExcl match {
      case None => cSQL""
      case Some(id: String) => cSQL"WHERE user_id > ${id}"
    }
    SQL"""SELECT user_id, primary_party
          FROM participant_users
          $whereClause
          ORDER BY user_id
          ${QueryStrategy.limitClause(Some(maxResults))}"""
      .asVectorOf(ParticipantUserParser2)(connection)
      .map { case (userId, primaryPartyRaw) =>
        toDomainUser(userId, dbStringToPartyString(primaryPartyRaw))
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
      case _ =>
        throw new RuntimeException(s"Could not convert ${(value, partyO)} to a user right.")
    }
  }

  private def fromUserRight(right: UserRight): (Int, Option[Party]) = {
    right match {
      case ParticipantAdmin => (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None)
      case CanActAs(party) => (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party))
      case CanReadAs(party) => (Right.CAN_READ_AS_FIELD_NUMBER, Some(party))
      case _ =>
        throw new RuntimeException(s"Could not recognize user right: $right.")
    }
  }

  private def dbStringToPartyString(raw: Option[String]): Option[Party] = {
    raw.map(Party.assertFromString)
  }

  private def isForPartyPredicate(forParty: Option[Party]): ComposableQuery.CompositeSql = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    forParty.fold(cSQL"IS NULL") { party: Party =>
      cSQL"= ${party: String}"
    }
  }

  private def toDomainUser(userId: String, primaryParty: Option[String]): domain.User = {
    domain.User(
      UserId.assertFromString(userId),
      primaryParty.map(Party.assertFromString),
    )
  }

}
