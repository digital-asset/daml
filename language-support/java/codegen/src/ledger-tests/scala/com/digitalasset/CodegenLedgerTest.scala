// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import java.io.File
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.Collections.singletonList
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Collections, UUID}

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{
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
import com.digitalasset.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass.{
  GetLedgerEndRequest,
  GetTransactionsResponse
}
import com.digitalasset.ledger.api.v1.{CommandServiceGrpc, TransactionServiceGrpc}
import com.digitalasset.platform.sandbox.SandboxApplication
import com.digitalasset.platform.sandbox.config.{DamlPackageContainer, LedgerIdMode, SandboxConfig}
import com.digitalasset.platform.sandbox.services.SandboxServerResource
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}
import io.grpc.Channel
import org.scalatest.{FlatSpec, Matchers}
import tests.wolpertinger.color.Grey
import tests.wolpertinger.{Color, Wolpertinger}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class CodegenLedgerTest extends FlatSpec with Matchers {

  def testDalf = new File("language-support/java/codegen/ledger-tests-model.dar")

  val LedgerID = "ledger-test"
  def withClient(testCode: Channel => Unit): Unit = {
    val cfg = SandboxConfig.default.copy(
      port = 0,
      damlPackageContainer = DamlPackageContainer(List(testDalf)),
      ledgerIdMode = LedgerIdMode.Predefined(LedgerID),
      timeProviderType = TimeProviderType.WallClock,
      timeModel = TimeModel.reasonableDefault
    )
    val sandbox = new SandboxServerResource(SandboxApplication(cfg))
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
      .flatMap[CreatedEvent]((e: Event) =>
        e match {
          case e: CreatedEvent => singletonList(e).stream();
          case _ => Collections.emptyList[CreatedEvent]().stream()
      })
      .map[Wolpertinger.Contract](e =>
        Wolpertinger.Contract.fromIdAndRecord(e.getContractId, e.getArguments))
      .collect(Collectors.toList[Wolpertinger.Contract])
      .asScala
      .toList
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
    "Glookofly",
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
    ()
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
    wolpertingers should have length 3

    val glook :: sruq :: glookosruq :: Nil = wolpertingers

    glook.data.name shouldEqual glookofly.name
    sruq.data.name shouldEqual sruquito.name
    glookosruq.data.name shouldEqual s"${glookofly.name}-${sruquito.name}"
    glookosruq.data.timeOfBirth shouldEqual tob
    ()
  }
}
