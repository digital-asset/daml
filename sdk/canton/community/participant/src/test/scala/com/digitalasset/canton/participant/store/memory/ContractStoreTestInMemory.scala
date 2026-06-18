// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.syntax.parallel.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.*
import org.scalatest.wordspec.AsyncWordSpec

class ContractStoreTestInMemory extends AsyncWordSpec with BaseTest with ContractStoreTest {

  "InMemoryContractStore" should {
    behave like contractStore(() => new InMemoryContractStore(timeouts, loggerFactory))
  }

  "store and retrieve a created contract with correct cache behavior" in {
    val store = new InMemoryContractStore(timeouts, loggerFactory)

    store.lookupPersistedIfCached(contractId) shouldBe Some(None)

    for {
      p0 <- store.lookupPersisted(contractId).failOnShutdown
      _ <- store.lookupPersistedIfCached(contractId) shouldBe Some(None)
      _ <- store.storeContract(contract).failOnShutdown
      p <- store.lookupPersisted(contractId).failOnShutdown
      c <- store.lookupE(contractId)
    } yield {
      p0 shouldBe None
      c shouldEqual contract
      p.value.asContractInstance shouldEqual contract
      store.lookupPersistedIfCached(contractId).value.value.asContractInstance shouldEqual contract
    }
  }

  "delete a set of contracts as done by pruning with correct cache behavior" in {
    val store = new InMemoryContractStore(timeouts, loggerFactory)
    store.lookupPersistedIfCached(contractId) shouldBe Some(None)
    for {
      _ <- List(contract, contract2, contract4, contract5)
        .parTraverse(store.storeContract)
        .failOnShutdown
      _ = store.lookupPersistedIfCached(contractId).value.nonEmpty shouldBe true
      _ <- store
        .deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
        .failOnShutdown
      _ = store.lookupPersistedIfCached(contractId) shouldBe Some(None)
      notFounds <- List(contractId, contractId2, contractId3, contractId4).parTraverse(
        store.lookupE(_).value
      )
      notDeleted <- store.lookupE(contractId5).value
    } yield {
      notFounds shouldEqual List(
        Left(UnknownContract(contractId)),
        Left(UnknownContract(contractId2)),
        Left(UnknownContract(contractId3)),
        Left(UnknownContract(contractId4)),
      )
      notDeleted shouldEqual Right(contract5)
      store.lookupPersistedIfCached(contractId) shouldBe Some(None) // already tried to be looked up
      store.lookupPersistedIfCached(contractId5).value.nonEmpty shouldBe true
    }
  }
}
