// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

trait UserManagementStore {
  import UserManagementStore._

  // read access

  def getUserInfo(id: Ref.UserId): Future[Result[UserInfo]]

  def listUsers(): Future[Result[Users]]

  // write access

  def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]]

  def deleteUser(id: Ref.UserId): Future[Result[Unit]]

  def grantRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  def revokeRights(id: Ref.UserId, rights: Set[UserRight]): Future[Result[Set[UserRight]]]

  // read helpers

  final def getUser(id: Ref.UserId): Future[Result[User]] = {
    getUserInfo(id).map(_.map(_.user))(ExecutionContext.parasitic)
  }

  final def listUserRights(id: Ref.UserId): Future[Result[Set[UserRight]]] = {
    getUserInfo(id).map(_.map(_.rights))(ExecutionContext.parasitic)
  }

}

object UserManagementStore {
  type Result[T] = Either[Error, T]
  type Users = Seq[User]

  case class UserInfo(user: User, rights: Set[UserRight])

  sealed trait Error extends RuntimeException
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
}
