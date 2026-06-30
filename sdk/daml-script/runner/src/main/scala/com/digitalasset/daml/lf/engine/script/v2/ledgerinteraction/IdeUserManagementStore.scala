// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction

import com.digitalasset.daml.ledger.client.{User, UserRight}
import com.digitalasset.daml.lf.data.Ref

import scala.collection.mutable
import scala.concurrent.Future

/** Minimal in-memory user-management store backing [[IdeLedgerClient]].
  *
  * Self-contained replacement for the canton ledger-api-core
  * `com.digitalasset.canton.ledger.localstore.InMemoryUserManagementStore`, trimmed to the subset
  * of behaviour used by the script-service ledger client (no identity-provider scoping, no resource
  * versioning, no annotation validation). A method returns `None` when the targeted user does not
  * exist (or, for [[createUser]], already exists).
  */
final class IdeUserManagementStore {

  private val state: mutable.TreeMap[Ref.UserId, IdeUserManagementStore.UserInfo] =
    mutable.TreeMap()

  def createUser(user: User, rights: Set[UserRight]): Future[Option[Unit]] =
    Future.successful(state.synchronized {
      if (state.contains(user.id)) None
      else {
        state.update(user.id, IdeUserManagementStore.UserInfo(user, rights))
        Some(())
      }
    })

  def getUser(id: Ref.UserId): Future[Option[User]] =
    Future.successful(state.synchronized(state.get(id).map(_.user)))

  def deleteUser(id: Ref.UserId): Future[Option[Unit]] =
    Future.successful(state.synchronized {
      if (state.remove(id).isDefined) Some(()) else None
    })

  def listAllUsers(): Future[List[User]] =
    Future.successful(state.synchronized(state.valuesIterator.map(_.user).toList))

  def grantRights(id: Ref.UserId, granted: Set[UserRight]): Future[Option[List[UserRight]]] =
    Future.successful(state.synchronized {
      state.get(id).map { info =>
        val newlyGranted = granted.diff(info.rights)
        state.update(id, info.copy(rights = info.rights ++ newlyGranted))
        newlyGranted.toList
      }
    })

  def revokeRights(id: Ref.UserId, revoked: Set[UserRight]): Future[Option[List[UserRight]]] =
    Future.successful(state.synchronized {
      state.get(id).map { info =>
        val effectivelyRevoked = revoked.intersect(info.rights)
        state.update(id, info.copy(rights = info.rights -- effectivelyRevoked))
        effectivelyRevoked.toList
      }
    })

  def listUserRights(id: Ref.UserId): Future[Option[List[UserRight]]] =
    Future.successful(state.synchronized(state.get(id).map(_.rights.toList)))

}

object IdeUserManagementStore {
  private final case class UserInfo(user: User, rights: Set[UserRight])
}
