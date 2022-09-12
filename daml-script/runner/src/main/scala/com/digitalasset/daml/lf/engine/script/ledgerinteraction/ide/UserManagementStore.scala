// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction.ide

import com.daml.lf.data.Ref.UserId
import com.daml.ledger.api.domain.{User, UserRight}

import scala.collection.mutable

// Non thread safe, this is only used for execution of a sinlge script which is
// single threaded.
// Note (MK) This is a simplified version of the in-memory store used in the participant in
// #11896. While it would be nice to not have to duplicate this for now this seems like the
// simpler option than trying to reuse participant code in the script service.
// TODO pbatko: Update vs. replace with participant impl vs. do-nothing?
private[ledgerinteraction] class UserManagementStore {
  import UserManagementStore._

  def createUser(user: User, rights: Set[UserRight]): Option[Unit] =
    putIfAbsent(UserInfo(user, rights)) match {
      case Some(_) => None
      case None => Some(())
    }

  def getUser(id: UserId): Option[User] =
    lookup(id) match {
      case Some(userInfo) => Some(userInfo.user)
      case None => None
    }

  def deleteUser(id: UserId): Option[Unit] =
    dropExisting(id) match {
      case Some(_) => Some(())
      case None => None
    }

  def grantRights(id: UserId, granted: Set[UserRight]): Option[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) =>
        val newlyGranted = granted.diff(userInfo.rights)
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted))
        Some(newlyGranted)
      case None =>
        None
    }

  def revokeRights(id: UserId, revoked: Set[UserRight]): Option[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) =>
        val effectivelyRevoked = revoked.intersect(userInfo.rights)
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked))
        Some(effectivelyRevoked)
      case None =>
        None
    }

  def listUserRights(id: UserId): Option[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) => Some(userInfo.rights)
      case None => None
    }

  def listUsers(): List[User] =
    state.values.map(_.user).toList

  private val state: mutable.Map[UserId, UserInfo] = mutable.Map()
  private def lookup(id: UserId) = state.get(id)
  private def dropExisting(id: UserId) = state.get(id).map(_ => state -= id)
  private def putIfAbsent(info: UserInfo) = {
    val old = state.get(info.user.id)
    if (old.isEmpty) state.update(info.user.id, info)
    old
  }
  private def replaceInfo(oldInfo: UserInfo, newInfo: UserInfo) = {
    assert(
      oldInfo.user.id == newInfo.user.id,
      s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}",
    )
    state.get(oldInfo.user.id) match {
      case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo)
      case _ => throw new IllegalArgumentException(s"User id not found ${oldInfo.user.id}")
    }
  }
}

object UserManagementStore {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (UserId, UserInfo) = user.id -> this
  }
}
