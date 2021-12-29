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

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.Map[Ref.UserId, UserInfo] = mutable.Map(AdminUser.toStateEntry)

  override def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]] =
    withoutUser(user.id) {
      state.update(user.id, UserInfo(user, rights))
    }

  override def getUser(id: Ref.UserId): Future[Result[User]] =
    withUser(id)(_.user)

  override def deleteUser(id: Ref.UserId): Future[Result[Unit]] =
    withUser(id) { _ =>
      state.remove(id)
      ()
    }

  override def grantRights(
      id: Ref.UserId,
      granted: Set[UserRight],
  ): Future[Result[Set[UserRight]]] =
    withUser(id) { userInfo =>
      val newlyGranted = granted.diff(userInfo.rights) // faster than filter
      // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
      assert(
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted))
      )
      newlyGranted
    }

  override def revokeRights(
      id: Ref.UserId,
      revoked: Set[UserRight],
  ): Future[Result[Set[UserRight]]] =
    withUser(id) { userInfo =>
      val effectivelyRevoked = revoked.intersect(userInfo.rights) // faster than filter
      // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
      assert(
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked))
      )
      effectivelyRevoked
    }

  override def listUserRights(id: Ref.UserId): Future[Result[Set[UserRight]]] =
    withUser(id)(_.rights)

  def listUsers(): Future[Result[Users]] =
    withState {
      Right(state.values.map(_.user).toSeq)
    }

  private def withState[T](t: => T): Future[T] =
    synchronized(
      Future.successful(t)
    )

  private def withUser[T](id: Ref.UserId)(f: UserInfo => T): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(user) => Right(f(user))
        case None => Left(UserNotFound(id))
      }
    )

  private def withoutUser[T](id: Ref.UserId)(t: => T): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(_) => Left(UserExists(id))
        case None => Right(t)
      }
    )

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
  // TODO participant user management: Review usage in PersistentUserManagementStore
  val AdminUser = UserInfo(
    user = User(Ref.UserId.assertFromString("participant_admin"), None),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
