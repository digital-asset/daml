// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.UserManagement._
import com.daml.ledger.participant.state.index.v2.UserManagementService

import scala.concurrent.Future

class InMemoryUserManagementService extends UserManagementService {
  import InMemoryUserManagementService._

  @volatile private var state: Map[String, UserInfo] = Map(AdminUser.toStateEntry)

  override def createUser(user: User, rights: Set[Right]): Future[Boolean] = synchronized {
    state.get(user.id) match {
      case Some(_) => Future.successful(false)
      case None =>
        state = state + UserInfo(user, rights).toStateEntry
        Future.successful(true)
    }
  }

  override def getUser(id: String): Future[User] =
    state.get(id) match {
      case Some(userInfo) => Future.successful(userInfo.user)
      case None => Future.failed(new Exception("not found"))
    }

  override def deleteUser(id: String): Future[Unit] = synchronized {
    state.get(id) match {
      case Some(_) =>
        state = state - id
        Future.unit
      case None =>
        Future.failed(new Exception("not found"))
    }
  }

  override def grantRights(id: String, rights: Set[Right]): Future[Set[Right]] = synchronized {
    state.get(id) match {
      case Some(userInfo) =>
        val newRights = rights
          .filterNot(userInfo.rights)
        state = state + userInfo.copy(rights = userInfo.rights ++ newRights).toStateEntry
        Future.successful(newRights)
      case None =>
        Future.failed(new Exception("not found"))
    }
  }

  override def revokeRights(id: String, rights: Set[Right]): Future[Set[Right]] = synchronized {
    state.get(id) match {
      case Some(userInfo) =>
        val newRevokeRights = rights
          .filter(userInfo.rights)
        state = state + userInfo.copy(rights = userInfo.rights -- newRevokeRights).toStateEntry
        Future.successful(newRevokeRights)
      case None =>
        Future.failed(new Exception("not found"))
    }
  }

  override def listUserRights(id: String): Future[Set[Right]] =
    state.get(id) match {
      case Some(userInfo) => Future.successful(userInfo.rights)
      case None => Future.failed(new Exception("not found"))
    }
}

object InMemoryUserManagementService {
  case class UserInfo(user: User, rights: Set[Right]) {
    def toStateEntry: (String, UserInfo) = user.id -> this
  }
  val AdminUser = UserInfo(
    user = User("admin", None),
    rights = Set(Right.ParticipantAdmin),
  )
}
