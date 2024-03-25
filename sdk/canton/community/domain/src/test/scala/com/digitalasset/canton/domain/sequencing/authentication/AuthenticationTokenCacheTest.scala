// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.mockito.ArgumentMatchers
import org.scalatest.wordspec.AsyncWordSpec

class AuthenticationTokenCacheTest extends AsyncWordSpec with BaseTest {
  class Env() {
    val store = new InMemoryMemberAuthenticationStore
    val storeSpy = spy(store)
    val clock = new SimClock(loggerFactory = loggerFactory)
    val cache = new AuthenticationTokenCache(clock, storeSpy, loggerFactory)
  }

  lazy val participant1 = DefaultTestIdentities.participant1
  lazy val participant2 = DefaultTestIdentities.participant2
  lazy val crypto = new SymbolicPureCrypto
  lazy val token1 = AuthenticationToken.generate(crypto)
  lazy val token2 = AuthenticationToken.generate(crypto)
  def ts(n: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(n.toLong)

  "lookup" should {
    "return empty if not in store" in {
      val env = new Env()

      for {
        result <- env.cache.lookupMatchingToken(participant1, token1)
      } yield result shouldBe empty
    }

    "return from store if present" in {
      val env = new Env()
      val storedToken = StoredAuthenticationToken(participant1, ts(5), token1)

      for {
        _ <- env.store.saveToken(storedToken)
        result <- env.cache.lookupMatchingToken(participant1, token1)
      } yield result.value shouldBe storedToken
    }

    "return empty if the store contains a token but it has already expired" in {
      val env = new Env()
      val tokenExpiry = ts(5)
      val storedToken = StoredAuthenticationToken(participant1, tokenExpiry, token1)

      env.clock.advanceTo(tokenExpiry)

      for {
        _ <- env.store.saveToken(storedToken)
        result <- env.cache.lookupMatchingToken(participant1, token1)
      } yield result shouldBe empty
    }

    "cache if found in store and then return from cache on next call" in {
      val env = new Env()
      val storedToken = StoredAuthenticationToken(participant1, ts(5), token1)

      for {
        _ <- env.store.saveToken(storedToken)
        firstTokenResult <- env.cache.lookupMatchingToken(participant1, token1)
        secondTokenResult <- env.cache.lookupMatchingToken(participant1, token1)
      } yield {
        firstTokenResult.value shouldBe storedToken
        secondTokenResult.value shouldBe storedToken

        // ensure the token lookup was only called once as on the second call it should be available in the cache
        verify(env.storeSpy, times(1)).fetchTokens(ArgumentMatchers.eq(participant1))(
          anyTraceContext
        )

        succeed
      }
    }
  }

  "invaliding all tokens" should {
    "invalid all the tokens for a member" in {
      val stored1 = StoredAuthenticationToken(participant1, ts(4), token1)
      val stored2 = StoredAuthenticationToken(participant1, ts(5), token2)
      val env = new Env()

      for {
        _ <- env.cache.saveToken(stored1)
        _ <- env.cache.saveToken(stored2)
        _ <- env.cache.invalidateAllTokensForMember(participant1)
        token1ResultO <- env.cache.lookupMatchingToken(participant1, token1)
        token2ResultO <- env.cache.lookupMatchingToken(participant1, token2)
        storedTokens <- env.store.fetchTokens(participant1)
      } yield {
        token1ResultO shouldBe empty
        token2ResultO shouldBe empty
        storedTokens shouldBe empty
      }
    }
  }

  "expiry" should {
    "clear cache and all tokens from persisted store when expiration is reached" in {
      val env = new Env()
      val expireAt = ts(5)
      val storedToken = StoredAuthenticationToken(participant1, expireAt, token1)

      for {
        _ <- env.cache.saveToken(storedToken)
        _ = env.clock.advanceTo(ts(4))
        resultBeforeO <- env.cache.lookupMatchingToken(participant1, token1)
        _ = resultBeforeO.value shouldBe storedToken
        _ = env.clock.advanceTo(expireAt)
        resultAfterO <- env.cache.lookupMatchingToken(participant1, token1)
        storedTokens <- env.store.fetchTokens(participant1)
      } yield {
        verify(env.storeSpy, times(1)).expireNoncesAndTokens(ArgumentMatchers.eq(expireAt))(
          anyTraceContext
        )

        resultAfterO shouldBe empty
        storedTokens shouldBe empty
      }
    }
  }
}
