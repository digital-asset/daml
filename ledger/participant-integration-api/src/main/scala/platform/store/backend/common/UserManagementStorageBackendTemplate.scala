// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.data.Ref.UserId
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.UserManagementStorageBackend

import scala.util.Try

object UserManagementStorageBackendTemplate extends UserManagementStorageBackend {

  /** Marker value for absence of party in a db table (`primary_party` and `for_party` columns).
    *
    * Oracle doesn't distinguish between empty strings and NULLs.
    * When we use NULL/empty string in Oracle then in Postgres we have two choices:
    * a) Choosing NULL: we would have to use partial index to ensure multi-column uniqueness constrains (`UNIQUE (user_internal_id, user_right, for_party)` index).
    * b) Choosing empty string: we would end up with DB aware backends having ot use `... is NULL` in Oracle vs. `... = ''` in Postgres
    */
  private val AbsenceOfPartyMarker = "!"

  private val ParticipantUserParser: RowParser[(Int, String, String)] =
    int("internal_id") ~ str("user_id") ~ str("primary_party") map {
      case internalId ~ userId ~ primaryParty =>
        (internalId, userId, primaryParty)
    }

  private val ParticipantUserParser2: RowParser[(String, String)] =
    str("user_id") ~ str("primary_party") map { case userId ~ primaryParty =>
      (userId, primaryParty)
    }

  private val UserRightParser: RowParser[(Int, String)] =
    int("user_right") ~ str("for_party") map { case user_right ~ for_party =>
      (user_right, for_party)
    }

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  override def createUser(user: domain.User)(
      connection: Connection
  ): Int = {
    val internalId: Try[Int] =
      SQL"""
         INSERT INTO participant_users (user_id, primary_party)
         VALUES (${user.id: String}, ${partyToDbString(user.primaryParty)})
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
      .map { case (internalId, userId, primaryPartyRaw) =>
        UserManagementStorageBackend.DbUser(
          internalId = internalId,
          domainUser = domain.User(
            id = Ref.UserId.assertFromString(userId),
            primaryParty = dbStringToPartyString(primaryPartyRaw),
          ),
        )
      }
  }

  override def getUsers()(connection: Connection): Vector[domain.User] = {
    def domainUser(userId: String, primaryParty: Option[String]): domain.User = {
      domain.User(
        Ref.UserId.assertFromString(userId),
        primaryParty.map(Ref.Party.assertFromString),
      )
    }
    SQL"""SELECT internal_id, user_id, primary_party
          FROM participant_users"""
      .asVectorOf(ParticipantUserParser2)(connection)
      .map { case (userId, primaryPartyRaw) =>
        domainUser(userId, dbStringToPartyString(primaryPartyRaw))
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
    val (userRight: Int, forParty: String) = fromUserRight(right)

    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
         SELECT 1 AS dummy
         FROM participant_user_rights ur
         WHERE ur.user_internal_id = ${internalId}
               AND
               ur.user_right = ${userRight}
               AND
               ur.for_party = $forParty""".asVectorOf(IntParser0)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

  override def addUserRight(internalId: Int, right: UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: String) = fromUserRight(right)
    val rowsUpdated: Int =
      SQL"""
         INSERT INTO participant_user_rights (user_internal_id, user_right, for_party)
         VALUES (
            ${internalId},
            ${userRight},
            ${forParty}
            )
         """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def getUserRights(internalId: Int)(connection: Connection): Set[domain.UserRight] = {
    val rec: Seq[(Int, String)] =
      SQL"""
         SELECT ur.user_right, ur.for_party
         FROM participant_user_rights ur
         WHERE ur.user_internal_id = ${internalId}
         """.asVectorOf(UserRightParser)(connection)
    rec.map { case (userRight, forPartyRaw) =>
      makeUserRight(
        value = userRight,
        partyRaw = forPartyRaw,
      )
    }.toSet
  }

  override def deleteUserRight(internalId: Int, right: domain.UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: String) = fromUserRight(right)
    val updatedRowCount: Int =
      SQL"""
           DELETE FROM participant_user_rights ur
           WHERE
            ur.user_internal_id = ${internalId}
            AND
            ur.user_right = ${userRight}
            AND
            ur.for_party = ${forParty}
           """.executeUpdate()(connection)
    updatedRowCount == 1
  }

  private def makeUserRight(value: Int, partyRaw: String): UserRight = {
    val partyO = dbStringToPartyString(partyRaw)
    (value, partyO) match {
      case (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None) => ParticipantAdmin
      case (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party)) => CanActAs(party)
      case (Right.CAN_READ_AS_FIELD_NUMBER, Some(party)) => CanReadAs(party)
      case _ =>
        throw new RuntimeException(s"Could not convert ${(value, partyO)} to a user right!")
    }
  }

  private def fromUserRight(right: UserRight): (Int, String) = {
    right match {
      case ParticipantAdmin => (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, AbsenceOfPartyMarker)
      case CanActAs(party) => (Right.CAN_ACT_AS_FIELD_NUMBER, party: String)
      case CanReadAs(party) => (Right.CAN_READ_AS_FIELD_NUMBER, party: String)
      case _ =>
        throw new RuntimeException(s"Could not recognize user right: $right!")
    }
  }

  private def dbStringToPartyString(raw: String): Option[Ref.Party] = {
    if (raw == AbsenceOfPartyMarker)
      None
    else
      Some(Ref.Party.assertFromString(raw))
  }

  private def partyToDbString(party: Option[Ref.Party]): String = {
    (party: Option[String]).getOrElse(AbsenceOfPartyMarker)
  }

}
