// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref

import scala.concurrent.Future

trait UserManagementStore {
  import UserManagementStore._

  def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]]

  def getUser(id: Ref.UserId): Future[Result[User]]

  def deleteUser(id: Ref.UserId): Future[Result[Unit]]

  def grantRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def revokeRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def listUserRights(id: Ref.UserId): Future[Result[Set[UserRight]]]

  def listUsers(): Future[Result[Users]]
}

object UserManagementStore {
  type Result[T] = Either[Error, T]
  type Users = Seq[User]

  sealed trait Error
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
}
