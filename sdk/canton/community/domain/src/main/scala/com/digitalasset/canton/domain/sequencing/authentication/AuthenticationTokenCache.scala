// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Provides a read and write through cache for authentication tokens while also
  * enforcing expiry timestamps based on the local clock.
  */
class AuthenticationTokenCache(
    clock: Clock,
    store: MemberAuthenticationStore,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val tokenCache = TrieMap[AuthenticationToken, StoredAuthenticationToken]()

  def lookupMatchingToken(member: Member, providedToken: AuthenticationToken)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredAuthenticationToken]] = {
    val now = clock.now

    def lookupFromStore(): Future[Option[StoredAuthenticationToken]] =
      for {
        matchedTokenO <- store
          .fetchTokens(member)
          .map(_.filter(_.expireAt > now).find(_.token == providedToken))
        // cache it
        _ = matchedTokenO.foreach(cacheToken)
      } yield matchedTokenO

    // lookup from cache, otherwise fetch from store (and then cache that)
    tokenCache.get(providedToken) match {
      case Some(token) if token.member == member && token.expireAt > now =>
        Future.successful(Some(token))
      case _ => lookupFromStore()
    }
  }

  /** Will persist and locally cache a new token */
  def saveToken(
      storedToken: StoredAuthenticationToken
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- store.saveToken(storedToken)
    } yield cacheToken(storedToken)

  /** Removes all tokens for the given member from the persisted store and cache.
    * Expected to be used when the member is disabled on the domain.
    */
  def invalidateAllTokensForMember(
      member: Member
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- store.invalidateMember(member)
      _ = tokenCache
        .filter(_._2.member == member)
        .keys
        .foreach(tokenCache.remove(_).discard[Option[StoredAuthenticationToken]])
    } yield ()

  private def cacheToken(stored: StoredAuthenticationToken): Unit = {
    tokenCache.putIfAbsent(stored.token, stored).discard

    val _ = clock.scheduleAt(
      _ => {
        TraceContext.withNewTraceContext { implicit traceContext =>
          logger.debug(s"Expiring token for ${stored.member}@${stored.expireAt}")
          tokenCache.remove(stored.token).discard
          FutureUtil
            .doNotAwait(store.expireNoncesAndTokens(clock.now), "Expiring old nonces and tokens")
        }
      },
      stored.expireAt,
    )
  }

}
