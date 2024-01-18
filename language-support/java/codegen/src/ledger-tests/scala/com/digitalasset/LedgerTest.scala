// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.math.BigDecimal
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneOffset}
import com.daml.ledger.javaapi.data.{Unit => DamlUnit}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Numeric
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import wolpertinger.color.Grey
import wolpertinger.{Color, Wolpertinger}
import alltests.MultiParty
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import io.grpc.Channel
import org.scalatest.Assertion

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import java.util.Arrays.asList

trait LedgerTest
    extends AsyncFlatSpecLike
    with TestLedger
    with Matchers
    with TestResourceContext
    with SuiteResourceManagementAroundAll {

  import TestUtil._

  def withUniqueParty(
      testCode: (String, Wolpertinger, Wolpertinger, Channel) => Assertion
  ): Future[Assertion] = for {
    alice <- allocateParty
    glookofly = new Wolpertinger(
      alice,
      3L,
      new BigDecimal("17.4200000000"),
      "Glookofly",
      true,
      LocalDate.of(1583, 12, 8),
      LocalDate.of(1583, 12, 8).atStartOfDay().toInstant(ZoneOffset.UTC),
      List[Wolpertinger.ContractId]().asJava,
      List[Color](new Grey(DamlUnit.getInstance())).asJava,
    )
    sruquito = new Wolpertinger(
      alice,
      1L,
      new BigDecimal("8.2000000000"),
      "Sruquito",
      true,
      LocalDate.of(1303, 3, 19),
      LocalDate.of(1303, 3, 19).atStartOfDay().toInstant(ZoneOffset.UTC),
      List[Wolpertinger.ContractId]().asJava,
      List[Color](new Grey(DamlUnit.getInstance())).asJava,
    )
    result <- withClient(testCode(alice, glookofly, sruquito, _))
  } yield result

  behavior of "Generated Java code"

  it should "create correct create commands" in withUniqueParty { (alice, glookofly, _, client) =>
    sendCmd(client, alice, glookofly.create())

    val glookoflyContract :: Nil =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

    glookoflyContract.data shouldEqual glookofly
  }

  it should "create correct exercise choice commands" in withUniqueParty {
    (alice, glookofly, sruquito, client) =>
      import java.util.Arrays.asList
      sendCmd(client, asList(alice), asList[String](), glookofly.create(), sruquito.create())

      val glookoflyContract :: sruquitoContract :: Nil =
        readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

      glookoflyContract.data shouldEqual glookofly
      sruquitoContract.data shouldEqual sruquito

      val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
      val reproduceUpdate = glookoflyContract.id.exerciseReproduce(sruquitoContract.id, tob)
      sendCmd(client, alice, reproduceUpdate)

      val wolpertingers = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)
      wolpertingers should have length 2

      println(wolpertingers)

      val sruq :: glookosruq :: Nil = wolpertingers

      sruq.data.name shouldEqual sruquito.name
      glookosruq.data.name shouldEqual s"${glookofly.name}-${sruquito.name}"
      glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "create correct createAndExercise choice commands" in withUniqueParty {
    (alice, glookofly, sruquito, client) =>
      sendCmd(client, alice, glookofly.create())

      val glookoflyContract :: Nil =
        readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

      glookoflyContract.data shouldEqual glookofly

      val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
      val reproduceUpdate = sruquito.createAnd.exerciseReproduce(glookoflyContract.id, tob)
      sendCmd(client, alice, reproduceUpdate)

      val wolpertingers = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)
      wolpertingers should have length 2

      val glook :: glookosruq :: Nil = wolpertingers

      glook.data.name shouldEqual glookofly.name
      glookosruq.data.name shouldEqual s"${sruquito.name}-${glookofly.name}"
      glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "provide the key" in withUniqueParty { (alice, glookofly, _, client) =>
    sendCmd(client, alice, glookofly.create())

    val wolpertinger :: _ =
      readActiveContractKeys(Wolpertinger.COMPANION)(client, alice)

    wolpertinger.isPresent shouldBe true
    wolpertinger.get.owner shouldEqual alice
    wolpertinger.get.age shouldEqual new BigDecimal("17.4200000000")
  }

  it should "be able to exercise by key" in withUniqueParty {
    (alice, glookofly, sruquito, client) =>
      sendCmd(client, alice, glookofly.create(), sruquito.create())

      // We'll exercise by key, no need to get the handles
      val glookoflyContract :: sruquitoContract :: Nil =
        readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

      val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
      val reproduceByKeyUpdate =
        Wolpertinger.byKey(glookoflyContract.key.get).exerciseReproduce(sruquitoContract.id, tob)
      sendCmd(client, alice, reproduceByKeyUpdate)

      val wolpertingers = readActiveContractPayloads(Wolpertinger.COMPANION)(client, alice)
      wolpertingers should have length 2

      val sruq :: glookosruq :: Nil = wolpertingers

      sruq.name shouldEqual sruquito.name
      glookosruq.name shouldEqual s"${glookofly.name}-${sruquito.name}"
      glookosruq.timeOfBirth shouldEqual tob
  }

  it should "provide the correct signatories" in withUniqueParty { (alice, glookofly, _, client) =>
    sendCmd(client, alice, glookofly.create())

    val wolpertinger :: _ =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

    // as stated explicitly in src/ledger-tests/daml/Wolpertinger.daml
    wolpertinger.signatories should contain only glookofly.owner
  }

  it should "provide the correct observers" in withUniqueParty { (alice, glookofly, _, client) =>
    sendCmd(client, alice, glookofly.create())

    val wolpertinger :: _ =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client, alice)

    // no explicit observers and the only choice controller is a signatory
    wolpertinger.observers shouldBe empty
  }

  it should "be able to create multi-party templates" in withClient { client =>
    for {
      List(alice, bob) <- Future.sequence(List.fill(2)(allocateParty))
      read = {
        val multi = new MultiParty(alice, bob)
        sendCmd(client, asList(alice, bob), asList[String](), multi.create())
        readActiveContracts(MultiParty.Contract.fromCreatedEvent)(client, alice).head
      }
      res = {
        read.data.p1 shouldBe alice
        read.data.p2 shouldBe bob
      }
    } yield res
  }

  it should "be able to read as other parties" in withClient { client =>
    for {
      List(alice, bob, charlie) <- Future.sequence(List.fill(3)(allocateParty))
      _ = {
        sendCmd(
          client,
          asList(charlie, bob),
          asList[String](),
          new MultiParty(charlie, bob).create(),
        )
        sendCmd(client, asList(alice, bob), asList[String](), new MultiParty(alice, bob).create())
        sendCmd(
          client,
          asList(alice),
          asList(charlie),
          MultiParty
            .byKey(new da.types.Tuple2(alice, bob))
            .exerciseMPFetchOtherByKey(charlie, bob),
        )
      }
    } yield succeed
  }

}
