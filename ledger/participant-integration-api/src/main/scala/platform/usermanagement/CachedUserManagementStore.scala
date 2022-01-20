// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Executor}

import com.daml.caching.CaffeineCache
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.User
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{Result, UserInfo, Users}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.metrics.Metrics
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CachedUserManagementStore(
    delegate: UserManagementStore,
    expiryAfterWriteInSeconds: Int,
    maximumCacheSize: Int,
    metrics: Metrics,
)(implicit val executionContext: ExecutionContext)
    extends UserManagementStore {

  private val cache: CaffeineCache.AsyncLoadingCaffeineCache[Ref.UserId, Result[UserInfo]] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new AsyncCacheLoader[UserId, Result[UserInfo]] {
            override def asyncLoad(
                key: UserId,
                executor: Executor,
            ): CompletableFuture[Result[UserInfo]] = {
              val cf = new CompletableFuture[Result[UserInfo]]
              delegate.getUserInfo(key).onComplete {
                case Success(value) => cf.complete(value)
                case Failure(e) => cf.completeExceptionally(e)
              }
              cf
            }
          }
        ),
      metrics.daml.userManagement.cache,
    )

  override def getUserInfo(id: UserId): Future[Result[UserManagementStore.UserInfo]] = {
    cache.get(id)
  }

  override def createUser(user: User, rights: Set[domain.UserRight]): Future[Result[Unit]] =
    delegate.createUser(user, rights)

  override def deleteUser(id: UserId): Future[Result[Unit]] = {
    delegate
      .deleteUser(id)
      .andThen(invalidateOnSuccess(id))
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .grantRights(id, rights)
      .andThen(invalidateOnSuccess(id))
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    delegate
      .revokeRights(id, rights)
      .andThen(invalidateOnSuccess(id))
  }

  override def listUsers(): Future[Result[Users]] = {
    delegate.listUsers()
  }

  private def invalidateOnSuccess[_](id: UserId): PartialFunction[Try[Result[Any]], Unit] = {
    case Success(Right(_)) => cache.invalidate(id)
  }

}
