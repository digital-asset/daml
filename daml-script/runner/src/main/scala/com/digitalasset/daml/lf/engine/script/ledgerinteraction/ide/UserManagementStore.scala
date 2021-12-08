// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction.ide

import com.daml.lf.data.Ref.UserId
import com.daml.lf.scenario.Error.UserManagementError
import com.daml.ledger.api.domain.{User, UserRight}

import scala.collection.mutable

// Non thread safe, this is only used for execution of a sinlge script which is
// single threaded.
// Note (MK) This is a simplified version of the in-memory store used in the participant in
// #11896. While it would be nice to not have to duplicate this for now this seems like the
// simpler option than trying to reuse participant code in the script service.
private[ledgerinteraction] class UserManagementStore {
  import UserManagementError._
  import UserManagementStore._

  def createUser(user: User, rights: Set[UserRight]): Result[Unit] =
    putIfAbsent(UserInfo(user, rights)) match {
      case Some(_) => Left(UserExists(user.id))
      case None => Right(())
    }

  def getUser(id: UserId): Result[User] =
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.user)
      case None => Left(UserNotFound(id))
    }

  def deleteUser(id: UserId): Result[Unit] =
    dropExisting(id) match {
      case Some(_) => Right(())
      case None => Left(UserNotFound(id))
    }

  def grantRights(id: UserId, granted: Set[UserRight]): Result[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) =>
        val newlyGranted = granted.diff(userInfo.rights)
        assert(replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted)))
        Right(newlyGranted)
      case None =>
        Left(UserNotFound(id))
    }

  def revokeRights(id: UserId, revoked: Set[UserRight]): Result[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) =>
        val effectivelyRevoked = revoked.intersect(userInfo.rights)
        assert(
          replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked))
        )
        Right(effectivelyRevoked)
      case None =>
        Left(UserNotFound(id))
    }

  def listUserRights(id: UserId): Result[Set[UserRight]] =
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.rights)
      case None => Left(UserNotFound(id))
    }

  def listUsers(): Result[List[User]] =
    Right(state.values.map(_.user).toList)

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
      case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
      case _ => false
    }
  }
}

object UserManagementStore {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (UserId, UserInfo) = user.id -> this
  }
  type Result[T] = Either[UserManagementError, T]
}
