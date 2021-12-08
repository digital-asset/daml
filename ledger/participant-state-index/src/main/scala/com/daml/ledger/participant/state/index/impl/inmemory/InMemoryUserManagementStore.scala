// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.impl.inmemory

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore._
import com.daml.lf.data.Ref

import scala.collection.mutable
import scala.concurrent.Future

class InMemoryUserManagementStore extends UserManagementStore {
  import InMemoryUserManagementStore._

  override def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]] =
    Future.successful {
      putIfAbsent(UserInfo(user, rights)) match {
        case Some(_) => Left(UserExists(user.id))
        case None => Right(())
      }
    }

  override def getUser(id: Ref.UserId): Future[Result[User]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.user)
      case None => Left(UserNotFound(id))
    }
  }

  override def deleteUser(id: Ref.UserId): Future[Result[Unit]] = Future.successful {
    dropExisting(id) match {
      case Some(_) => Right(())
      case None => Left(UserNotFound(id))
    }
  }

  override def grantRights(
      id: Ref.UserId,
      granted: Set[UserRight],
  ): Future[Result[Set[UserRight]]] =
    Future.successful {
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

  override def revokeRights(
      id: Ref.UserId,
      revoked: Set[UserRight],
  ): Future[Result[Set[UserRight]]] =
    Future.successful {
      lookup(id) match {
        case Some(userInfo) =>
          val effectivelyRevoked = revoked.intersect(userInfo.rights) // faster than filter
          // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
          assert(
            replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked))
          )
          Right(effectivelyRevoked)
        case None =>
          Left(UserNotFound(id))
      }
    }

  override def listUserRights(id: Ref.UserId): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.rights)
      case None => Left(UserNotFound(id))
    }
  }

  def listUsers(): Future[Result[Users]] = Future.successful {
    Right(state.values.map(_.user).toSeq)
  }

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.Map[Ref.UserId, UserInfo] = mutable.Map(AdminUser.toStateEntry)
  private def lookup(id: Ref.UserId) = state.synchronized { state.get(id) }
  private def dropExisting(id: Ref.UserId) = state.synchronized {
    state.get(id).map(_ => state -= id)
  }
  private def putIfAbsent(info: UserInfo) = state.synchronized {
    val old = state.get(info.user.id)

    if (old.isEmpty) state.update(info.user.id, info)

    old
  }
  private def replaceInfo(oldInfo: UserInfo, newInfo: UserInfo) = state.synchronized {
    assert(
      oldInfo.user.id == newInfo.user.id,
      s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}",
    )
    state.get(oldInfo.user.id) match {
      case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
      case _ => false
    }
  }
}

object InMemoryUserManagementStore {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (Ref.UserId, UserInfo) = user.id -> this
  }
  private val AdminUser = UserInfo(
    user = User(Ref.UserId.assertFromString("participant_admin"), None),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
