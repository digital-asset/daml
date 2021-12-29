// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import java.time.Duration

import com.daml.caching.{CaffeineCache, ConcurrentCache}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{
  Result,
  TooManyUserRights,
  UserExists,
  UserNotFound,
  Users,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.github.benmanes.caffeine.cache.Caffeine

import scala.concurrent.{ExecutionContext, Future}

object CachedUserManagementStore {

  // TODO participant user management: copied from InMemory version
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (Ref.UserId, UserInfo) = user.id -> this
  }

  val ExpiryAfterWriteInSeconds: Int = 10
}

class CachedUserManagementStore(
    private val delegate: UserManagementStore,
    expiryAfterWriteInSeconds: Int,
)(implicit val executionContext: ExecutionContext)
    extends UserManagementStore {

  private val usersCache: ConcurrentCache[UserId, User] = CaffeineCache[Ref.UserId, User](
    builder = Caffeine
      .newBuilder()
      .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
      // TODO participant user management: Check the choice of the maximum size
      .maximumSize(10000),
    // TODO participant user management: Use metrics
    metrics = None,
  )

  import com.github.benmanes.caffeine.cache.Caffeine

  private val userRightsCache: ConcurrentCache[UserId, Set[UserRight]] =
    CaffeineCache[Ref.UserId, Set[UserRight]](
      builder = Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        // TODO participant user management: Check the choice of the maximum size
        .maximumSize(10000),
      // TODO participant user management: Use metrics
      metrics = None,
    )

  // TODO copied from Persistent version
  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r match {
      case Right(v) => f(v)
      case Left(error) =>
        error match {
          case UserNotFound(userId) =>
            // TODO participant user management: We don't need to invalidate here as deleteUser() already handles it
            usersCache.invalidate(userId)
            userRightsCache.invalidate(userId)
          case _: UserExists =>
          case _: TooManyUserRights =>
        }
    }
    r
  }

  override def createUser(user: domain.User, rights: Set[domain.UserRight]): Future[Result[Unit]] =
    delegate
      .createUser(user, rights)
      .map(tapSuccess { _ =>
        usersCache.put(user.id, user)
        userRightsCache.put(user.id, rights)
      })

  override def getUser(id: UserId): Future[Result[domain.User]] = {
    usersCache
      .getIfPresent(id)
      .fold(
        delegate
          .getUser(id)
          .map(tapSuccess(user => usersCache.put(id, user)))
      )((user) => Future.successful(Right(user)))
  }

  override def deleteUser(id: UserId): Future[Result[Unit]] = {
    delegate
      .deleteUser(id)
      .map(tapSuccess { _ =>
        usersCache.invalidate(id)
        userRightsCache.invalidate(id)
      })
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .grantRights(id, rights)
      .map(
        tapSuccess(granted =>
          userRightsCache
            .getIfPresent(id)
            .foreach(cachedRights => userRightsCache.put(id, cachedRights.union(granted)))
        )
      )
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .revokeRights(id, rights)
      .map(
        tapSuccess(revoked =>
          userRightsCache
            .getIfPresent(id)
            .foreach(cachedRights => userRightsCache.put(id, cachedRights.diff(revoked)))
        )
      )
  }

  override def listUserRights(id: UserId): Future[Result[Set[domain.UserRight]]] = {
    userRightsCache
      .getIfPresent(id)
      .fold(
        delegate.listUserRights(id).map(tapSuccess(rights => userRightsCache.put(id, rights)))
      )(rights => Future.successful(Right(rights)))

  }

  override def listUsers(): Future[Result[Users]] = {
    delegate.listUsers()
  }
}
