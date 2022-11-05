// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import java.sql.Connection

import com.daml.ledger.api.domain.UserRight
import com.daml.lf.data.Ref
import com.daml.platform.UserId

trait UserManagementStorageBackend extends ResourceVersionOps {

  def createUser(user: UserManagementStorageBackend.DbUserPayload)(connection: Connection): Int

  def addUserAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit

  def deleteUserAnnotations(internalId: Int)(connection: Connection): Unit

  def getUserAnnotations(internalId: Int)(connection: Connection): Map[String, String]

  def deleteUser(id: UserId)(connection: Connection): Boolean

  def getUser(id: UserId)(connection: Connection): Option[UserManagementStorageBackend.DbUserWithId]

  def getUsersOrderedById(fromExcl: Option[UserId] = None, maxResults: Int)(
      connection: Connection
  ): Vector[UserManagementStorageBackend.DbUserWithId]

  def addUserRight(internalId: Int, right: UserRight, grantedAt: Long)(
      connection: Connection
  ): Unit

  /** @return true if the right existed and we have just deleted it.
    */
  def deleteUserRight(internalId: Int, right: UserRight)(connection: Connection): Boolean

  def userRightExists(internalId: Int, right: UserRight)(connection: Connection): Boolean

  def getUserRights(internalId: Int)(
      connection: Connection
  ): Set[UserManagementStorageBackend.DbUserRight]

  def countUserRights(internalId: Int)(connection: Connection): Int

  def updateUserPrimaryParty(internalId: Int, primaryPartyO: Option[Ref.Party])(
      connection: Connection
  ): Boolean

  def updateUserIsDeactivated(
      internalId: Int,
      isDeactivated: Boolean,
  )(connection: Connection): Boolean

}

object UserManagementStorageBackend {
  case class DbUserPayload(
      id: Ref.UserId,
      primaryPartyO: Option[Ref.Party],
      identityProviderId: Option[Ref.IdentityProviderId],
      isDeactivated: Boolean,
      resourceVersion: Long,
      createdAt: Long,
  )

  case class DbUserWithId(
      internalId: Int,
      payload: DbUserPayload,
  )
  case class DbUserRight(domainRight: UserRight, grantedAt: Long)
}
