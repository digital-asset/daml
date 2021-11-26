// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import scala.concurrent.Future

import com.daml.ledger.api.UserManagement._


trait UserManagementService {
  import UserManagementService._

  def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]]

  def getUser(id: String): Future[Result[User]]

  def deleteUser(id: String): Future[Result[Unit]]

  def grantRights(id: String, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def revokeRights(id: String, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def listUserRights(id: String): Future[Result[Set[UserRight]]]
}

object UserManagementService {
  type Result[T] = Either[Error, T]

  sealed trait Error
  final case class UserNotFound(userId: String) extends Error
  final case class UserExists(userId: String) extends Error
}
