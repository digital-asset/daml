// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Executor}

import com.daml.caching.{AsyncLoadingCache, CaffeineCache}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.User
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{Result, UserInfo, Users}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CachedUserManagementStore(
    private val delegate: UserManagementStore,
    expiryAfterWriteInSeconds: Int,
)(implicit val executionContext: ExecutionContext)
    extends UserManagementStore {

  // TODO participant user management: Use metrics (instrumented cache)
  val cache: AsyncLoadingCache[Ref.UserId, Result[UserInfo]] =
    new CaffeineCache.SimpleAsyncLoadingCache(
      com.github.benmanes.caffeine.cache.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        // TODO participant user management: Check the choice of the maximum size
        .maximumSize(10000)
        .buildAsync(
          new com.github.benmanes.caffeine.cache.AsyncCacheLoader[Ref.UserId, Result[UserInfo]] {
            override def asyncLoad(
                key: Ref.UserId,
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
        )
    )

  override def getUserInfo(id: UserId): Future[Result[UserManagementStore.UserInfo]] = {
    cache.get(id)
  }

  override def createUser(user: User, rights: Set[domain.UserRight]): Future[Result[Unit]] =
    delegate.createUser(user, rights)

  override def deleteUser(id: UserId): Future[Result[Unit]] = {
    cache.invalidate(id)
    // TODO pbatko: Invalidating cache after a successful write to ensure cache gets refreshed. Prevents
    //              a case when there is a read just after the above eager cache invalidation. Need this?
    //              Prevents the cache from being stale for 10s in a rare case of a such a race.
    //              Leaning towards dropping eager cache invalidation.
    delegate.deleteUser(id).map(tapInvalidateOnSuccess(id))
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    cache.invalidate(id)
    delegate
      .grantRights(id, rights)
      .map(tapInvalidateOnSuccess(id))
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
  ): Future[Result[Set[domain.UserRight]]] = {
    cache.invalidate(id)
    delegate
      .revokeRights(id, rights)
      .map(tapInvalidateOnSuccess(id))
  }

  override def listUsers(): Future[Result[Users]] = {
    delegate.listUsers()
  }

  private def tapInvalidateOnSuccess[T](id: UserId)(r: Result[T]): Result[T] = {
    r match {
      case Right(_) => cache.invalidate(id)
      case Left(_) =>
    }
    r
  }

}
