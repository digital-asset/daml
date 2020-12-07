// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.math.BigDecimal
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneOffset}

import com.daml.ledger.javaapi.data.{Unit => DamlUnit}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Numeric
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import wolpertinger.color.Grey
import wolpertinger.{Color, Wolpertinger}

import scala.collection.JavaConverters._

class CodegenLedgerTest extends AsyncFlatSpec with Matchers with TestResourceContext {

  import TestUtil._

  val glookofly = new Wolpertinger(
    Alice,
    3L,
    new BigDecimal("17.4200000000"),
    "Glookofly",
    true,
    LocalDate.of(1583, 12, 8),
    LocalDate.of(1583, 12, 8).atStartOfDay().toInstant(ZoneOffset.UTC),
    List[Wolpertinger.ContractId]().asJava,
    List[Color](new Grey(DamlUnit.getInstance())).asJava
  )

  val sruquito = new Wolpertinger(
    Alice,
    1L,
    new BigDecimal("8.2000000000"),
    "Sruquito",
    true,
    LocalDate.of(1303, 3, 19),
    LocalDate.of(1303, 3, 19).atStartOfDay().toInstant(ZoneOffset.UTC),
    List[Wolpertinger.ContractId]().asJava,
    List[Color](new Grey(DamlUnit.getInstance())).asJava
  )

  behavior of "Generated Java code"

  it should "create correct create commands" in withClient { client =>
    sendCmd(client, glookofly.create())

    val glookoflyContract :: Nil =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    glookoflyContract.data shouldEqual glookofly
  }

  it should "create correct exercise choice commands" in withClient { client =>
    sendCmd(client, glookofly.create(), sruquito.create())

    val glookoflyContract :: sruquitoContract :: Nil =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    glookoflyContract.data shouldEqual glookofly
    sruquitoContract.data shouldEqual sruquito

    val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
    val reproduceCmd = glookoflyContract.id
      .exerciseReproduce(sruquitoContract.id, tob)
    sendCmd(client, reproduceCmd)

    val wolpertingers = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)
    wolpertingers should have length 2

    println(wolpertingers)

    val sruq :: glookosruq :: Nil = wolpertingers

    sruq.data.name shouldEqual sruquito.name
    glookosruq.data.name shouldEqual s"${glookofly.name}-${sruquito.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "create correct createAndExercise choice commands" in withClient { client =>
    sendCmd(client, glookofly.create())

    val glookoflyContract :: Nil =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    glookoflyContract.data shouldEqual glookofly

    val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
    val reproduceCmd = sruquito.createAndExerciseReproduce(glookoflyContract.id, tob)
    sendCmd(client, reproduceCmd)

    val wolpertingers = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)
    wolpertingers should have length 2

    val glook :: glookosruq :: Nil = wolpertingers

    glook.data.name shouldEqual glookofly.name
    glookosruq.data.name shouldEqual s"${sruquito.name}-${glookofly.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "provide the agreement text" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    wolpertinger.agreementText.isPresent shouldBe true
    wolpertinger.agreementText.get shouldBe s"${wolpertinger.data.name} has ${wolpertinger.data.wings} wings and is ${Numeric
      .toUnscaledString(Numeric.assertFromUnscaledBigDecimal(wolpertinger.data.age))} years old."
  }

  it should "provide the key" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    wolpertinger.key.isPresent shouldBe true
    wolpertinger.key.get.owner shouldEqual "Alice"
    wolpertinger.key.get.age shouldEqual new BigDecimal("17.4200000000")
  }

  it should "be able to exercise by key" in withClient { client =>
    sendCmd(client, glookofly.create(), sruquito.create())

    // We'll exercise by key, no need to get the handles
    val glookoflyContract :: sruquitoContract :: Nil =
      readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
    val reproduceByKeyCmd =
      Wolpertinger.exerciseByKeyReproduce(glookoflyContract.key.get, sruquitoContract.id, tob)
    sendCmd(client, reproduceByKeyCmd)

    val wolpertingers = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)
    wolpertingers should have length 2

    val sruq :: glookosruq :: Nil = wolpertingers

    sruq.data.name shouldEqual sruquito.name
    glookosruq.data.name shouldEqual s"${glookofly.name}-${sruquito.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "provide the correct signatories" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    // as stated explicitly in src/ledger-tests/daml/Wolpertinger.daml
    wolpertinger.signatories should contain only glookofly.owner
  }

  it should "provide the correct observers" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(Wolpertinger.Contract.fromCreatedEvent)(client)

    // no explicit observers and the only choice controller is a signatory
    wolpertinger.observers shouldBe empty
  }

}
