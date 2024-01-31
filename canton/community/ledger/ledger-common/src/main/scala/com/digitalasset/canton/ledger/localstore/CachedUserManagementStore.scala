// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.caching.CaffeineCache.FutureAsyncCacheLoader
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User}
import com.digitalasset.canton.ledger.localstore.api.{UserManagementStore, UserUpdate}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.github.benmanes.caffeine.cache as caffeine

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

import CachedUserManagementStore.CacheKey
import UserManagementStore.{Result, UserInfo}

class CachedUserManagementStore(
    delegate: UserManagementStore,
    expiryAfterWriteInSeconds: Int,
    maximumCacheSize: Int,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContextWithTrace)
    extends UserManagementStore
    with NamedLogging {

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
      metrics.userManagement.cache,
    )

  override def getUserInfo(id: UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserManagementStore.UserInfo]] = {
    cache.get(CacheKey(id, identityProviderId))
  }

  override def createUser(user: User, rights: Set[domain.UserRight])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]] =
    delegate
      .createUser(user, rights)
      .andThen(invalidateOnSuccess(CacheKey(user.id, user.identityProviderId)))

  override def updateUser(
      userUpdate: UserUpdate
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] = {
    delegate
      .updateUser(userUpdate)
      .andThen(invalidateOnSuccess(CacheKey(userUpdate.id, userUpdate.identityProviderId)))
  }

  override def deleteUser(
      id: UserId,
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Unit]] = {
    delegate
      .deleteUser(id, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .grantRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .revokeRights(id, rights, identityProviderId)
      .andThen(invalidateOnSuccess(CacheKey(id, identityProviderId)))
  }

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserManagementStore.UsersPage]] =
    delegate.listUsers(fromExcl, maxResults, identityProviderId)

  override def updateUserIdp(
      id: UserId,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] = {
    val keyToInvalidate = CacheKey(id, sourceIdp)
    delegate
      .updateUserIdp(id, sourceIdp = sourceIdp, targetIdp = targetIdp)
      .andThen(invalidateOnSuccess(keyToInvalidate))
  }

  private def invalidateOnSuccess(key: CacheKey): PartialFunction[Try[Result[Any]], Unit] = {
    case Success(Right(_)) => cache.invalidate(key)
  }

}

object CachedUserManagementStore {
  final case class CacheKey(id: UserId, identityProviderId: IdentityProviderId)
}
