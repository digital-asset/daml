// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import org.scalatest.wordspec.AsyncWordSpec

class MemberAuthenticationStoreTest extends AsyncWordSpec with BaseTest {
  lazy val participant1 = DefaultTestIdentities.participant1
  lazy val participant2 = DefaultTestIdentities.participant2
  lazy val participant3 = DefaultTestIdentities.participant3
  lazy val defaultExpiry = CantonTimestamp.Epoch.plusSeconds(120)
  lazy val crypto = new SymbolicPureCrypto

  "invalid member" should {
    "work fine if the member has no active token" in {
      val store = mk()

      store.invalidateMember(participant1)
      val tokens = store.fetchTokens(participant1)

      tokens shouldBe empty
    }

    "remove token and nonce for participant" in {
      val store = mk()

      store.saveToken(generateToken(participant1))
      val storedNonce = generateNonce(participant1)
      store.saveNonce(storedNonce)

      store.invalidateMember(participant1)
      val tokens = store.fetchTokens(participant1)
      val nonceO = store.fetchAndRemoveNonce(participant1, storedNonce.nonce)

      tokens shouldBe empty
      nonceO shouldBe empty
    }
  }

  "saving nonces" should {
    "support many for a member and remove the nonce when fetched" in {
      val store = mk()

      val nonce1 = generateNonce(participant1)
      val nonce2 = generateNonce(participant1)
      val unstoredNonce = generateNonce(participant1)

      store.saveNonce(nonce1)
      store.saveNonce(nonce2)

      val nonExistentNonceO = store.fetchAndRemoveNonce(participant1, unstoredNonce.nonce)
      val fetchedNonce1O = store.fetchAndRemoveNonce(participant1, nonce1.nonce)
      val fetchedNonce2O = store.fetchAndRemoveNonce(participant1, nonce2.nonce)
      val refetchedNonce1O = store.fetchAndRemoveNonce(participant1, nonce1.nonce)
      val refetchedNonce2O = store.fetchAndRemoveNonce(participant1, nonce2.nonce)

      nonExistentNonceO shouldBe empty
      fetchedNonce1O.value shouldBe nonce1
      fetchedNonce2O.value shouldBe nonce2
      refetchedNonce1O shouldBe empty
      refetchedNonce2O shouldBe empty
    }
  }

  "saving tokens" should {
    "support many for a member" in {
      val store = mk()

      val p1t1 = generateToken(participant1)
      val p1t2 = generateToken(participant1)
      val p2t1 = generateToken(participant2)

      List(p1t1, p1t2, p2t1).foreach(store.saveToken)

      val p1Tokens = store.fetchTokens(participant1)
      val p2Tokens = store.fetchTokens(participant2)
      val p3Tokens = store.fetchTokens(participant3)

      p1Tokens should contain.only(p1t1, p1t2)
      p2Tokens should contain.only(p2t1)
      p3Tokens shouldBe empty
    }
  }

  "expire" should {
    "expire all nonces and tokens at or before the given timestamp" in {
      val store = mk()

      val n1 = generateNonce(participant1, defaultExpiry.plusSeconds(-1))
      val n2 = generateNonce(participant1, defaultExpiry)
      val n3 = generateNonce(participant1, defaultExpiry.plusSeconds(1))
      val t1 = generateToken(participant1, defaultExpiry.plusSeconds(-1))
      val t2 = generateToken(participant1, defaultExpiry)
      val t3 = generateToken(participant1, defaultExpiry.plusSeconds(1))

      List(n1, n2, n3).foreach(store.saveNonce)
      List(t1, t2, t3).foreach(store.saveToken)

      store.expireNoncesAndTokens(defaultExpiry)
      val fn1O = store.fetchAndRemoveNonce(participant1, n1.nonce)
      val fn2O = store.fetchAndRemoveNonce(participant1, n2.nonce)
      val fn3O = store.fetchAndRemoveNonce(participant1, n3.nonce)
      val tokens = store.fetchTokens(participant1)

      fn1O shouldBe empty
      fn2O shouldBe empty
      fn3O.value shouldBe n3
      tokens should contain.only(t3)
    }
  }

  private def mk(): MemberAuthenticationStore = new MemberAuthenticationStore()

  private def generateToken(
      member: Member,
      expiry: CantonTimestamp = defaultExpiry,
  ): StoredAuthenticationToken =
    StoredAuthenticationToken(member, expiry, AuthenticationToken.generate(crypto))

  private def generateNonce(member: Member, expiry: CantonTimestamp = defaultExpiry): StoredNonce =
    StoredNonce(member, Nonce.generate(crypto), CantonTimestamp.Epoch, expiry)
}
