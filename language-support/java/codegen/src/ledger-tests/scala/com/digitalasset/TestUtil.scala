// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data._
import com.daml.ledger.participant.state.v1.TimeModel
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass.{
  GetLedgerEndRequest,
  GetTransactionsResponse
}
import com.digitalasset.ledger.api.v1.{CommandServiceGrpc, TransactionServiceGrpc}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.SandboxClientResource
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.Assertion

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

object TestUtil {

  def testDalf =
    new File(BazelRunfiles.rlocation("language-support/java/codegen/ledger-tests-model.dar"))

  val LedgerID = "ledger-test"

  def withClient(testCode: Channel => Assertion)(
      implicit executionContext: ExecutionContext
  ): Future[Assertion] = {
    val config = SandboxConfig.default.copy(
      port = 0,
      damlPackages = List(testDalf),
      ledgerIdMode = LedgerIdMode.Static(LedgerId(LedgerID)),
      timeProviderType = Some(TimeProviderType.WallClock),
      timeModel = TimeModel.reasonableDefault,
    )

    val channelOwner = for {
      server <- SandboxServer.owner(config)
      channel <- SandboxClientResource.owner(server.port)
    } yield channel
    channelOwner.use(channel => Future(testCode(channel)))
  }

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

  val Alice = "Alice"
  val Bob = "Bob"
  val Charlie = "Charlie"
  val allTemplates = new FiltersByParty(Map[String, Filter](Alice -> NoFilter.instance).asJava)

  def sendCmd(channel: Channel, cmds: Command*): Empty = {
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

  def readActiveContracts[C <: Contract](fromCreatedEvent: CreatedEvent => C)(
      channel: Channel
  ): List[C] = {
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
      .foldLeft(Map[String, C]())((acc, event) =>
        event match {
          case e: CreatedEvent =>
            acc + (e.getContractId -> fromCreatedEvent(e))
          case a: ArchivedEvent => acc - a.getContractId
      })
      .toList
      .sortBy(_._1)
      .map(_._2)
  }

}
