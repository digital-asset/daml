// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import java.io.File
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{
  ArchivedEvent,
  Command,
  CreatedEvent,
  Event,
  Filter,
  FiltersByParty,
  GetTransactionsRequest,
  LedgerOffset,
  NoFilter,
  SubmitCommandsRequest,
  Transaction,
  Unit => DamlUnit
}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass.{
  GetLedgerEndRequest,
  GetTransactionsResponse
}
import com.digitalasset.ledger.api.v1.{CommandServiceGrpc, TransactionServiceGrpc}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.{DamlPackageContainer, SandboxConfig}
import com.digitalasset.platform.sandbox.services.SandboxServerResource
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}
import io.grpc.Channel
import org.scalatest.{Assertion, FlatSpec, Matchers}
import tests.wolpertinger.color.Grey
import tests.wolpertinger.{Color, Wolpertinger}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import com.digitalasset.ledger.api.domain.LedgerId

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CodegenLedgerTest extends FlatSpec with Matchers with BazelRunfiles {

  def testDalf = new File(rlocation("language-support/java/codegen/ledger-tests-model.dar"))

  val LedgerID = "ledger-test"
  def withClient(testCode: Channel => Assertion): Assertion = {
    val cfg = SandboxConfig.default.copy(
      port = 0,
      damlPackageContainer = DamlPackageContainer(List(testDalf)),
      ledgerIdMode = LedgerIdMode.Static(LedgerId(LedgerID)),
      timeProviderType = TimeProviderType.WallClock,
      timeModel = TimeModel.reasonableDefault
    )
    val sandbox = new SandboxServerResource(cfg)
    sandbox.setup()
    try {
      testCode(sandbox.value)
    } finally {
      sandbox.close()
    }
  }

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

  val Alice = "Alice"
  val allTemplates = new FiltersByParty(Map[String, Filter](Alice -> NoFilter.instance).asJava)

  def sendCmd(channel: Channel, cmds: Command*) = {
    CommandServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(40, TimeUnit.SECONDS)
      .submitAndWait(
        SubmitAndWaitRequest
          .newBuilder()
          .setCommands(
            SubmitCommandsRequest.toProto(
              LedgerID,
              randomId,
              randomId,
              randomId,
              Alice,
              Instant.now,
              Instant.now.plusSeconds(10),
              cmds.asJava))
          .build)
  }

  def readActiveContracts(channel: Channel) = {
    val txService = TransactionServiceGrpc.newBlockingStub(channel)
    val end = txService.getLedgerEnd(GetLedgerEndRequest.newBuilder().setLedgerId(LedgerID).build)
    val txs = txService.getTransactions(
      new GetTransactionsRequest(
        LedgerID,
        LedgerOffset.LedgerBegin.getInstance(),
        LedgerOffset.fromProto(end.getOffset),
        allTemplates,
        true).toProto)
    val iterable: java.lang.Iterable[GetTransactionsResponse] = () => txs
    StreamSupport
      .stream(iterable.spliterator(), false)
      .flatMap[Transaction](
        (r: GetTransactionsResponse) =>
          data.GetTransactionsResponse
            .fromProto(r)
            .getTransactions
            .stream())
      .flatMap[Event]((t: Transaction) => t.getEvents.stream)
      .collect(Collectors.toList[Event])
      .asScala
      .foldLeft(Map[String, Wolpertinger.Contract]())((acc, event) =>
        event match {
          case e: CreatedEvent =>
            acc + (e.getContractId -> Wolpertinger.Contract.fromCreatedEvent(e))
          case a: ArchivedEvent => acc - a.getContractId
      })
      .toList
      .sortBy(_._1)
      .map(_._2)
  }

  val glookofly = new Wolpertinger(
    Alice,
    3L,
    BigDecimal(17.42).bigDecimal,
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
    BigDecimal(8.2).bigDecimal,
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

    val glookoflyContract :: Nil = readActiveContracts(client)

    glookoflyContract.data shouldEqual glookofly
  }

  it should "create correct exercise choice commands" in withClient { client =>
    sendCmd(client, glookofly.create(), sruquito.create())

    val glookoflyContract :: sruquitoContract :: Nil = readActiveContracts(client)

    glookoflyContract.data shouldEqual glookofly
    sruquitoContract.data shouldEqual sruquito

    val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
    val reproduceCmd = glookoflyContract.id
      .exerciseReproduce(sruquitoContract.id, tob)
    sendCmd(client, reproduceCmd)

    val wolpertingers = readActiveContracts(client)
    wolpertingers should have length 2

    println(wolpertingers)

    val sruq :: glookosruq :: Nil = wolpertingers

    sruq.data.name shouldEqual sruquito.name
    glookosruq.data.name shouldEqual s"${glookofly.name}-${sruquito.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "create correct createAndExercise choice commands" in withClient { client =>
    sendCmd(client, glookofly.create())

    val glookoflyContract :: Nil = readActiveContracts(client)

    glookoflyContract.data shouldEqual glookofly

    val tob = Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0)
    val reproduceCmd = sruquito.createAndExerciseReproduce(glookoflyContract.id, tob)
    sendCmd(client, reproduceCmd)

    val wolpertingers = readActiveContracts(client)
    wolpertingers should have length 2

    val glook :: glookosruq :: Nil = wolpertingers

    glook.data.name shouldEqual glookofly.name
    glookosruq.data.name shouldEqual s"${sruquito.name}-${glookofly.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
  }

  it should "provide the agreement text" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(client)

    wolpertinger.agreementText.isPresent shouldBe true
    wolpertinger.agreementText.get shouldBe s"${wolpertinger.data.name} has ${wolpertinger.data.wings} wings and is ${wolpertinger.data.age} years old."
  }

  it should "provide the key" in withClient { client =>
    sendCmd(client, glookofly.create())

    val wolpertinger :: _ = readActiveContracts(client)

    wolpertinger.key.isPresent shouldBe true
    wolpertinger.key.get.owner shouldEqual "Alice"
    wolpertinger.key.get.age shouldEqual java.math.BigDecimal.valueOf(17.42)
  }
}
