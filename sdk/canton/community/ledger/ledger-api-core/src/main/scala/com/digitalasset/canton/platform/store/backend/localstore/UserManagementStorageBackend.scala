// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.localstore

import com.digitalasset.canton.ledger.api.{IdentityProviderId, UserRight}
import com.digitalasset.canton.platform.UserId
import com.digitalasset.daml.lf.data.Ref

import java.sql.Connection

trait UserManagementStorageBackend extends ResourceVersionOps {

  def createUser(user: UserManagementStorageBackend.DbUserPayload)(connection: Connection): Int

  def addUserAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit

  def deleteUserAnnotations(internalId: Int)(connection: Connection): Unit

  def getUserAnnotations(internalId: Int)(connection: Connection): Map[String, String]

  def deleteUser(id: UserId)(connection: Connection): Boolean

  def getUser(id: UserId)(connection: Connection): Option[UserManagementStorageBackend.DbUserWithId]

  def getUsersOrderedById(
      fromExcl: Option[UserId] = None,
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(
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

  def updateUserIdp(internalId: Int, identityProviderId: Option[IdentityProviderId.Id])(
      connection: Connection
  ): Boolean

  def updateUserIsDeactivated(
      internalId: Int,
      isDeactivated: Boolean,
  )(connection: Connection): Boolean

}

object UserManagementStorageBackend {
  final case class DbUserPayload(
      id: Ref.UserId,
      primaryPartyO: Option[Ref.Party],
      identityProviderId: Option[IdentityProviderId.Id],
      isDeactivated: Boolean,
      resourceVersion: Long,
      createdAt: Long,
  )

  final case class DbUserWithId(
      internalId: Int,
      payload: DbUserPayload,
  )
  final case class DbUserRight(apiRight: UserRight, grantedAt: Long)
}
