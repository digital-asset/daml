// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

abstract class UserManagementStore()(implicit executionContext: ExecutionContext) {
  import UserManagementStore._

  def getUserInfo(id: Ref.UserId): Future[Result[UserInfo]]

  def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]]

  def listUsers(): Future[Result[Users]]

  def deleteUser(id: Ref.UserId): Future[Result[Unit]]

  def grantRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def revokeRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  final def getUser(id: Ref.UserId): Future[Result[User]] = {
    getUserInfo(id).map(_.map(_.user))
  }

  final def listUserRights(id: Ref.UserId): Future[Result[Set[UserRight]]] = {
    getUserInfo(id).map(_.map(_.rights))
  }

}

object UserManagementStore {
  type Result[T] = Either[Error, T]
  type Users = Seq[User]

  case class UserInfo(user: User, rights: Set[UserRight])

  sealed trait Error
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
}
