// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{int, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.UserRight
import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.UserManagementStorageBackend

import scala.util.Try

object UserManagementStorageBackendTemplate extends UserManagementStorageBackend {

  private val ParticipantUserParser: RowParser[(Int, String, Option[String])] =
    int("internal_id") ~ str("user_id") ~ str("primary_party").? map {
      case internalId ~ userId ~ primaryParty =>
        (internalId, userId, primaryParty)
    }

  private val ParticipantUserParser2: RowParser[(String, Option[String])] =
    str("user_id") ~ str("primary_party").? map { case userId ~ primaryParty =>
      (userId, primaryParty)
    }

  private val UserRightParser: RowParser[(Int, Option[String])] =
    int("user_right") ~ str("for_party").? map { case user_right ~ for_party =>
      (user_right, for_party)
    }

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  override def createUser(user: domain.User, createdAt: Long)(
      connection: Connection
  ): Int = {
    val internalId: Try[Int] =
      SQL"""
         INSERT INTO participant_users (user_id, primary_party, created_at)
         VALUES (${user.id: String}, ${user.primaryParty: Option[String]}, ${createdAt})
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
      .map { case (internalId, userId, primaryParty) =>
        UserManagementStorageBackend.DbUser(
          internalId = internalId,
          domainUser = domain.User(
            id = Ref.UserId.assertFromString(userId),
            primaryParty = primaryParty.map(Ref.Party.assertFromString),
          ),
        )
      }
  }

  override def getUsers()(connection: Connection): Vector[domain.User] = {
    def toDomainUser(userId: String, primaryParty: Option[String]): domain.User = {
      domain.User(
        Ref.UserId.assertFromString(userId),
        primaryParty.map(Ref.Party.assertFromString),
      )
    }
    SQL"""SELECT internal_id, user_id, primary_party
          FROM participant_users"""
      .asVectorOf(ParticipantUserParser2)(connection)
      .map { case (userId, primaryParty) =>
        toDomainUser(userId, primaryParty)
      }
  }

  override def deleteUser(userId: Ref.UserId)(connection: Connection): Boolean = {
    val updatedRowsCount =
      SQL"""
         DELETE FROM  participant_users WHERE user_id = ${userId: String}
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
  ): Boolean = {
    val (userRight: Int, forParty: Option[Party]) = fromUserRight(right)
    val rowsUpdated: Int =
      SQL"""
         INSERT INTO participant_user_rights (user_internal_id, user_right, for_party, granted_at)
         VALUES (
            ${internalId},
            ${userRight},
            ${forParty: Option[String]},
            ${grantedAt}
            )
         """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def getUserRights(internalId: Int)(connection: Connection): Set[domain.UserRight] = {
    val rec: Seq[(Int, Option[String])] =
      SQL"""
         SELECT ur.user_right, ur.for_party
         FROM participant_user_rights ur
         WHERE ur.user_internal_id = ${internalId}
         """.asVectorOf(UserRightParser)(connection)
    rec.map { case (userRight, forParty) =>
      makeUserRight(
        value = userRight,
        party = forParty.map(Ref.Party.assertFromString),
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

  private def isForPartyPredicate(forParty: Option[Party]): ComposableQuery.CompositeSql = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    forParty.fold(cSQL"IS NULL") { party: Party =>
      cSQL"= ${party: String}"
    }
  }

  private def makeUserRight(value: Int, party: Option[Party]): UserRight = {
    (value, party) match {
      case (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None) => ParticipantAdmin
      case (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party)) => CanActAs(party)
      case (Right.CAN_READ_AS_FIELD_NUMBER, Some(party)) => CanReadAs(party)
      case _ =>
        throw new RuntimeException // TODO participant user management: Use self-service error codes
    }
  }

  private def fromUserRight(right: UserRight): (Int, Option[Party]) = {
    right match {
      case ParticipantAdmin => (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None)
      case CanActAs(party) => (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party))
      case CanReadAs(party) => (Right.CAN_READ_AS_FIELD_NUMBER, Some(party))
      case _ =>
        throw new RuntimeException // TODO participant user management: Use self-service error codes
    }
  }

}
