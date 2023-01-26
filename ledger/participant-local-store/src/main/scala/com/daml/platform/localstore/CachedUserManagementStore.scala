// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderId, User}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.CachedUserManagementStore.CacheKey
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

  private val cache: CaffeineCache.AsyncLoadingCaffeineCache[CacheKey, Result[UserInfo]] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[CacheKey, Result[UserInfo]](key =>
            delegate.getUserInfo(key.id, key.identityProviderId)
          )
        ),
      metrics.daml.userManagement.cache,
    )

  override def getUserInfo(id: UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserManagementStore.UserInfo]] = {
    cache.get(CacheKey(id, identityProviderId))
  }

  override def createUser(user: User, rights: Set[domain.UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]] =
    delegate
      .createUser(user, rights)
      .andThen(invalidateOnSuccess(CacheKey(user.id, user.identityProviderId)))

  override def updateUser(
      userUpdate: UserUpdate
  )(implicit loggingContext: LoggingContext): Future[Result[User]] = {
    delegate
      .updateUser(userUpdate)
      .andThen(invalidateOnSuccess(CacheKey(userUpdate.id, userUpdate.identityProviderId)))
  }

  override def deleteUser(
      id: UserId,
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Unit]] = {
    delegate
      .deleteUser(id, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .grantRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .revokeRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserManagementStore.UsersPage]] =
    delegate.listUsers(fromExcl, maxResults, identityProviderId)

  private def invalidateOnSuccess(key: CacheKey): PartialFunction[Try[Result[Any]], Unit] = {
    case Success(Right(_)) => cache.invalidate(key)
  }

}

object CachedUserManagementStore {
  case class CacheKey(id: UserId, identityProviderId: IdentityProviderId)
}
