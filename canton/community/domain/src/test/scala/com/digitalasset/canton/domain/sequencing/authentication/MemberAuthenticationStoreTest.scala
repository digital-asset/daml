// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait MemberAuthenticationStoreTest extends AsyncWordSpec with BaseTest {
  lazy val participant1 = DefaultTestIdentities.participant1
  lazy val participant2 = DefaultTestIdentities.participant2
  lazy val participant3 = DefaultTestIdentities.participant3
  lazy val defaultExpiry = CantonTimestamp.Epoch.plusSeconds(120)
  lazy val crypto = new SymbolicPureCrypto

  def memberAuthenticationStore(mk: () => MemberAuthenticationStore): Unit = {
    "invalid member" should {
      "work fine if the member has no active token" in {
        val store = mk()

        for {
          _ <- store.invalidateMember(participant1)
          tokens <- store.fetchTokens(participant1)
        } yield tokens shouldBe empty
      }

      "remove token and nonce for participant" in {
        val store = mk()
        for {
          _ <- store.saveToken(generateToken(participant1))
          storedNonce = generateNonce(participant1)
          _ <- store.saveNonce(storedNonce)
          _ <- store.invalidateMember(participant1)
          tokens <- store.fetchTokens(participant1)
          nonceO <- store.fetchAndRemoveNonce(participant1, storedNonce.nonce)
        } yield {
          tokens shouldBe empty
          nonceO shouldBe empty
        }
      }
    }

    "saving nonces" should {
      "support many for a member and remove the nonce when fetched" in {
        val store = mk()

        val nonce1 = generateNonce(participant1)
        val nonce2 = generateNonce(participant1)
        val unstoredNonce = generateNonce(participant1)

        for {
          _ <- store.saveNonce(nonce1)
          _ <- store.saveNonce(nonce2)
          nonExistentNonceO <- store.fetchAndRemoveNonce(participant1, unstoredNonce.nonce)
          fetchedNonce1O <- store.fetchAndRemoveNonce(participant1, nonce1.nonce)
          fetchedNonce2O <- store.fetchAndRemoveNonce(participant1, nonce2.nonce)
          refetchedNonce1O <- store.fetchAndRemoveNonce(participant1, nonce1.nonce)
          refetchedNonce2O <- store.fetchAndRemoveNonce(participant1, nonce2.nonce)
        } yield {
          nonExistentNonceO shouldBe empty
          fetchedNonce1O.value shouldBe nonce1
          fetchedNonce2O.value shouldBe nonce2
          refetchedNonce1O shouldBe empty
          refetchedNonce2O shouldBe empty
        }
      }
    }

    "saving tokens" should {
      "support many for a member" in {
        val store = mk()

        val p1t1 = generateToken(participant1)
        val p1t2 = generateToken(participant1)
        val p2t1 = generateToken(participant2)

        for {
          _ <- List(p1t1, p1t2, p2t1).parTraverse(store.saveToken)
          p1Tokens <- store.fetchTokens(participant1)
          p2Tokens <- store.fetchTokens(participant2)
          p3Tokens <- store.fetchTokens(participant3)
        } yield {
          p1Tokens should contain.only(p1t1, p1t2)
          p2Tokens should contain.only(p2t1)
          p3Tokens shouldBe empty
        }
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

        for {
          _ <- List(n1, n2, n3).parTraverse(store.saveNonce)
          _ <- List(t1, t2, t3).parTraverse(store.saveToken)
          _ <- store.expireNoncesAndTokens(defaultExpiry)
          fn1O <- store.fetchAndRemoveNonce(participant1, n1.nonce)
          fn2O <- store.fetchAndRemoveNonce(participant1, n2.nonce)
          fn3O <- store.fetchAndRemoveNonce(participant1, n3.nonce)
          tokens <- store.fetchTokens(participant1)
        } yield {
          fn1O shouldBe empty
          fn2O shouldBe empty
          fn3O.value shouldBe n3
          tokens should contain.only(t3)
        }
      }
    }
  }

  def generateToken(
      member: Member,
      expiry: CantonTimestamp = defaultExpiry,
  ): StoredAuthenticationToken =
    StoredAuthenticationToken(member, expiry, AuthenticationToken.generate(crypto))
  def generateNonce(member: Member, expiry: CantonTimestamp = defaultExpiry): StoredNonce =
    StoredNonce(member, Nonce.generate(crypto), CantonTimestamp.Epoch, expiry)
}

class MemberAuthenticationStoreTestInMemory extends MemberAuthenticationStoreTest {
  "InMemoryMemberAuthenticationStore" should {
    behave like (memberAuthenticationStore(() => new InMemoryMemberAuthenticationStore))
  }
}

trait DbMemberAuthenticationStoreTest extends MemberAuthenticationStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    storage.update_(
      DBIO.seq(
        Seq("nonces", "tokens").map(name =>
          sqlu"truncate table sequencer_authentication_#$name"
        ): _*
      ),
      functionFullName,
    )
  }

  "DbMemberAuthenticationStore" should {
    behave like memberAuthenticationStore(() =>
      new DbMemberAuthenticationStore(storage, timeouts, loggerFactory, closeContext)
    )
  }
}

class MemberAuthenticationStoreTestH2 extends DbMemberAuthenticationStoreTest with H2Test
class MemberAuthenticationStoreTestPostgres
    extends DbMemberAuthenticationStoreTest
    with PostgresTest
