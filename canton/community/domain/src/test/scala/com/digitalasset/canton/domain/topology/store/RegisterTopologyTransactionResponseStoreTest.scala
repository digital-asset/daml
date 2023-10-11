// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.domain.topology.store.RegisterTopologyTransactionResponseStore.Response
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, TestingIdentityFactory}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait RegisterTopologyTransactionResponseStoreTest {
  this: AsyncWordSpec with BaseTest =>
  private val p1 = ParticipantId("p1")
  private val da = DomainId.tryFromString("da::default")

  private val requestId1 = String255.tryCreate("requestId1")
  private val requestId2 = String255.tryCreate("requestId2")

  private val response1 = RegisterTopologyTransactionResponse(
    p1,
    p1,
    requestId1,
    List(),
    da,
    testedProtocolVersion,
  )
  private val response2 = RegisterTopologyTransactionResponse(
    p1,
    p1,
    requestId2,
    List(),
    da,
    testedProtocolVersion,
  )

  def registerTopologyTransactionResponseStore(
      mk: () => RegisterTopologyTransactionResponseStore
  ): Unit = {

    "when storing responses" should {
      "be able to list responses" in {
        val sut = mk()
        for {
          _ <- sut.savePendingResponse(response1)
          _ <- sut.savePendingResponse(response2)
          resps <- sut.pendingResponses()
        } yield {
          resps.toSet shouldBe Set(response1, response2)
        }
      }

      "be able to check that there already exist a response for the given request id" in {
        val sut = mk()
        for {
          exists <- sut.exists(requestId1)
          _ = exists shouldBe false
          _ <- sut.savePendingResponse(response1)
          exists <- sut.exists(requestId1)
          _ = exists shouldBe true
        } yield succeed
      }

      "check whether response has been successfully sent" in {
        val sut = mk()
        for {
          _ <- sut.savePendingResponse(response1)
          responseO <- sut.getResponse(requestId1).value
          _ = responseO shouldBe Some(Response(response1, isCompleted = false))
          _ <- sut.completeResponse(requestId1)
          responseO <- sut.getResponse(requestId1).value
        } yield responseO shouldBe Some(Response(response1, isCompleted = true))
      }
    }
  }
}

class RegisterTopologyTransactionResponseStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RegisterTopologyTransactionResponseStoreTest {
  "RegisterTopologyTransactionResponseStoreTestInMemory" should {
    behave like registerTopologyTransactionResponseStore(() =>
      new InMemoryRegisterTopologyTransactionResponseStore()
    )
  }
}

trait DbRegisterTopologyTransactionResponseStoreTest
    extends AsyncWordSpec
    with BaseTest
    with RegisterTopologyTransactionResponseStoreTest {
  this: DbTest =>

  val pureCryptoApi: CryptoPureApi = TestingIdentityFactory.pureCrypto()

  def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update_(
      sqlu"truncate table register_topology_transaction_responses",
      functionFullName,
    )
  }
  "DbRegisterTopologyTransactionResponseStoreTest" should {
    behave like registerTopologyTransactionResponseStore(() =>
      new DbRegisterTopologyTransactionResponseStore(
        storage,
        pureCryptoApi,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class RegisterTopologyTransactionResponseStoreTestH2
    extends DbRegisterTopologyTransactionResponseStoreTest
    with H2Test

class RegisterTopologyTransactionResponseStoreTestPostgres
    extends DbRegisterTopologyTransactionResponseStoreTest
    with PostgresTest
