// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.{ApplicationId, User, UserRight}
import scala.concurrent.Future


trait UserManagementService {
  import UserManagementService._

  def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]]

  def getUser(id: ApplicationId): Future[Result[User]]

  def deleteUser(id: ApplicationId): Future[Result[Unit]]

  def grantRights(id: ApplicationId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def revokeRights(id: ApplicationId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def listUserRights(id: ApplicationId): Future[Result[Set[UserRight]]]

  def listUsers(/* TODO: pagination -- pageSize: Int, pageToken: String*/): Future[Result[Users]]
}

object UserManagementService {
  type Result[T] = Either[Error, T]
  type Users = Seq[User] // TODO: pagination -- change to something like case class PaginatedUsers(users: Seq[User], nextPageToken: String)

  sealed trait Error
  final case class UserNotFound(userId: ApplicationId) extends Error
  final case class UserExists(userId: ApplicationId) extends Error
}
