// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.UserManagement._
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.ledger.participant.state.index.v2.UserManagementService._

import scala.concurrent.Future
import scala.collection.mutable

class InMemoryUserManagementService extends UserManagementService {
  import InMemoryUserManagementService._

  override def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]] = Future.successful {
    putIfAbsent(UserInfo(user, rights)) match {
      case Some(_) => Left(UserExists(user.id))
      case None => Right(())
    }
  }

  override def getUser(id: String): Future[Result[User]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.user)
      case None => Left(UserNotFound(id))
    }
  }

  override def deleteUser(id: String): Future[Result[Unit]] = Future.successful {
    dropExisting(id) match {
      case Some(_) => Right(())
      case None => Left(UserNotFound(id))
    }
  }

  override def grantRights(id: String, granted: Set[UserRight]): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) =>
        val newlyGranted = granted.diff(userInfo.rights) // faster than filter
        // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
        assert(replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted)))
        Right(newlyGranted)
      case None =>
        Left(UserNotFound(id))
    }
  }

  override def revokeRights(id: String, revoked: Set[UserRight]): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) =>
        val effectivelyRevoked = revoked.intersect(userInfo.rights) // faster than filter
        // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
        assert(replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked)))
        Right(effectivelyRevoked)
      case None =>
        Left(UserNotFound(id))
    }
  }

  override def listUserRights(id: String): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.rights)
      case None => Left(UserNotFound(id))
    }
  }

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.Map[String, UserInfo] = mutable.Map(AdminUser.toStateEntry)
  private def lookup(id: String) = state.synchronized { state.get(id) }
  private def dropExisting(id: String) = state.synchronized { state.get(id).map(_ => state -= id) }
  private def putIfAbsent(info: UserInfo) = state.synchronized {
    val old = state.get(info.user.id)

    if (old.isEmpty) state.update(info.user.id, info)

    old
  }
  private def replaceInfo(oldInfo: UserInfo, newInfo: UserInfo) = state.synchronized {
    assert(oldInfo.user.id == newInfo.user.id, s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}")
    state.get(oldInfo.user.id) match {
      case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
      case _ => false
    }
  }
}

object InMemoryUserManagementService {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (String, UserInfo) = user.id -> this
  }
  private val AdminUser = UserInfo(
    user = User("participant_admin", None),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
