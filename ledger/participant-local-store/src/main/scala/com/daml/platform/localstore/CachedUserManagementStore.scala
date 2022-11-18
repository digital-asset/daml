// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.User
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.UserManagementStore.{Result, UserInfo}
import com.daml.platform.localstore.api.{UserManagementStore, UserUpdate}
import com.github.benmanes.caffeine.{cache => caffeine}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class CachedUserManagementStore(
    delegate: UserManagementStore,
    expiryAfterWriteInSeconds: Int,
    maximumCacheSize: Int,
    metrics: Metrics,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends UserManagementStore {

  private val cache: CaffeineCache.AsyncLoadingCaffeineCache[Ref.UserId, Result[UserInfo]] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[UserId, Result[UserInfo]](key =>
            delegate.getUserInfo(key, Ref.IdentityProviderId.Default)
          )
        ),
      metrics.daml.userManagement.cache,
    )

  override def getUserInfo(id: UserId, identityProviderId: Ref.IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserManagementStore.UserInfo]] = {
    cache
      .get(id)
      .map(_.flatMap {
        case user if identityProviderId == Ref.IdentityProviderId.Default => Right(user)
        case user if identityProviderId == user.user.identityProviderId =>
          Right(user)
        case _ =>
          Left(UserManagementStore.UserNotFound(id))
      })
  }

  override def createUser(user: User, rights: Set[domain.UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]] =
    delegate
      .createUser(user, rights)
      .andThen(invalidateOnSuccess(user.id))

  override def updateUser(
      userUpdate: UserUpdate
  )(implicit loggingContext: LoggingContext): Future[Result[User]] = {
    delegate
      .updateUser(userUpdate)
      .andThen(invalidateOnSuccess(userUpdate.id))
  }

  override def deleteUser(
      id: UserId,
      identityProviderId: Ref.IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Unit]] = {
    delegate
      .deleteUser(id, identityProviderId)
      .andThen(invalidateOnSuccess(id))
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: Ref.IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .grantRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(id))
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: Ref.IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .revokeRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(id))
  }

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: Ref.IdentityProviderId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserManagementStore.UsersPage]] =
    delegate.listUsers(fromExcl, maxResults, identityProviderId)

  private def invalidateOnSuccess(id: UserId): PartialFunction[Try[Result[Any]], Unit] = {
    case Success(Right(_)) => cache.invalidate(id)
  }

}
