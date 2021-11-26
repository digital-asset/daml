// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.UserManagement._
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.ledger.participant.state.index.v2.UserManagementService._

import scala.concurrent.Future

class InMemoryUserManagementService extends UserManagementService {
  import InMemoryUserManagementService._

  @volatile private var state: Map[String, UserInfo] = Map(AdminUser.toStateEntry)

  override def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]] = synchronized {
    state.get(user.id) match {
      case Some(_) => Future.successful(Left(UserExists(user.id)))
      case None =>
        state = state + UserInfo(user, rights).toStateEntry
        Future.successful(Right(()))
    }
  }

  override def getUser(id: String): Future[Result[User]] =
    Future.successful(
      state.get(id) match {
        case Some(userInfo) => Right(userInfo.user)
        case None => Left(UserNotFound(id))
      }
    )

  override def deleteUser(id: String): Future[Result[Unit]] = synchronized {
    Future.successful(state.get(id) match {
      case Some(_) =>
        state = state - id
        Right(())
      case None =>
        Left(UserNotFound(id))
    })
  }

  override def grantRights(id: String, rights: Set[UserRight]): Future[Result[Set[UserRight]]] =
    synchronized {
      state.get(id) match {
        case Some(userInfo) =>
          val newRights = rights
            .filterNot(userInfo.rights)
          state = state + userInfo.copy(rights = userInfo.rights ++ newRights).toStateEntry
          Future.successful(Right(newRights))
        case None =>
          Future.successful(Left(UserNotFound(id)))
      }
    }

  override def revokeRights(id: String, rights: Set[UserRight]): Future[Result[Set[UserRight]]] =
    synchronized {
      state.get(id) match {
        case Some(userInfo) =>
          val newRevokeRights = rights
            .filter(userInfo.rights)
          state = state + userInfo.copy(rights = userInfo.rights -- newRevokeRights).toStateEntry
          Future.successful(Right(newRevokeRights))
        case None =>
          Future.successful(Left(UserNotFound(id)))
      }
    }

  override def listUserRights(id: String): Future[Result[Set[UserRight]]] =
    state.get(id) match {
      case Some(userInfo) => Future.successful(Right(userInfo.rights))
      case None => Future.successful(Left(UserNotFound(id)))
    }

}

object InMemoryUserManagementService {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (String, UserInfo) = user.id -> this
  }
  private val AdminUser = UserInfo(
    user = User("admin", None),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
