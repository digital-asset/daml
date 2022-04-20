// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.api.domain
import com.daml.lf.data.Ref.UserId
import com.daml.platform.store.backend.UserManagementStorageBackend

object MUserManagementStorageBackend extends UserManagementStorageBackend {
  override def createUser(user: domain.User, createdAt: Long)(connection: Connection): Int =
    throw new UnsupportedOperationException

  override def deleteUser(id: UserId)(connection: Connection): Boolean =
    throw new UnsupportedOperationException

  override def getUser(id: UserId)(
      connection: Connection
  ): Option[UserManagementStorageBackend.DbUser] =
    throw new UnsupportedOperationException

  override def getUsersOrderedById(fromExcl: Option[UserId], maxResults: Int)(
      connection: Connection
  ): Vector[domain.User] =
    throw new UnsupportedOperationException

  override def addUserRight(internalId: Int, right: domain.UserRight, grantedAt: Long)(
      connection: Connection
  ): Unit =
    throw new UnsupportedOperationException

  override def deleteUserRight(internalId: Int, right: domain.UserRight)(
      connection: Connection
  ): Boolean =
    throw new UnsupportedOperationException

  override def userRightExists(internalId: Int, right: domain.UserRight)(
      connection: Connection
  ): Boolean =
    throw new UnsupportedOperationException

  override def getUserRights(internalId: Int)(
      connection: Connection
  ): Set[UserManagementStorageBackend.DbUserRight] =
    throw new UnsupportedOperationException

  override def countUserRights(internalId: Int)(connection: Connection): Int =
    throw new UnsupportedOperationException

  override def userManagementStorageBackendSupported: Boolean = false
}
