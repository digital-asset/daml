// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.acs

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.dvp.asset.Asset
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.jdk.CollectionConverters.*

class MyContractStore(
    index: M.dvp.asset.Asset.Contract => Party,
    filter: M.dvp.asset.Asset.Contract => Boolean,
    loggerFactory: NamedLoggerFactory,
) extends ContractStore[
      M.dvp.asset.Asset.Contract,
      M.dvp.asset.Asset.ContractId,
      M.dvp.asset.Asset,
      Party,
    ](
      "test",
      M.dvp.asset.Asset.COMPANION,
      index,
      filter,
      loggerFactory,
    ) {

  def addArchive(contractId: Asset.ContractId): Unit = this.processArchive_(contractId)

  def addCreate(create: M.dvp.asset.Asset.Contract): Unit =
    this.processCreate_(create)

}

class ContractStoreTest extends AnyWordSpec with BaseTest {

  private val issuer = new Party("issuer")
  private val alice = new Party("alice")
  private val bob = new Party("bob")
  private val charlie = new Party("charlie")

  private def genAsset(cid: String, owner: Party) =
    new Asset.Contract(
      new Asset.ContractId(cid),
      new M.dvp.asset.Asset(
        issuer.getValue,
        issuer.getValue,
        UUID.randomUUID().toString,
        "p",
        owner.getValue,
        owner.getValue,
      ),
      Set().asJava,
      Set().asJava,
    )

  private val aliceA = genAsset("1", alice)
  private val bobA = genAsset("2", bob)
  private val charlieA = genAsset("3", charlie)

  private def genStore() =
    new MyContractStore(
      index = x => new Party(x.data.owner),
      filter = x => x.data.owner != charlie.getValue,
      loggerFactory,
    )

  "contract store" should {

    "correctly observe creations" in {
      val st = genStore()

      st.addCreate(aliceA)
      st.addCreate(bobA)
      st.addCreate(charlieA)

      st.allAvailable.toList should have length (2)
      st.num(alice) shouldBe 1
      st.num(bob) shouldBe 1
      st.num(charlie) shouldBe 0
      st.one(alice) should be(Some(aliceA))

      st.find(alice) should have length (1)
      st.find(bob) should have length (1)
      st.find(charlie) shouldBe empty

    }

    "correctly observe archivals" in {
      val st = genStore()
      st.addCreate(aliceA)
      st.addCreate(bobA)
      st.addArchive(aliceA.id)

      st.allAvailable.toList should have length 1
      st.num(alice) shouldBe 0
      st.num(bob) shouldBe 1

      st.find(alice) shouldBe empty

    }

    "filter out when pending" in {
      val st = genStore()
      st.addCreate(aliceA)
      st.addCreate(bobA)
      st.setPending(aliceA.id, isPending = true)
      st.allAvailable.toList should have length 1
      st.num(alice) shouldBe 0

      st.setPending(aliceA.id, isPending = true)
      st.num(alice) shouldBe 0

      st.setPending(aliceA.id, isPending = false)
      st.num(alice) shouldBe 1

    }

    "correctly adjust pending when archived" in {
      val st = genStore()
      st.addCreate(aliceA)
      st.setPending(aliceA.id, isPending = true)
      st.addArchive(aliceA.id)
      st.num(alice) shouldBe 0
    }

  }

}
