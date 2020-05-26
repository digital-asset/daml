// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.io.File
import java.time.{Duration, Instant}
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data._
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.daml.ledger.api.v1.TransactionServiceOuterClass.{
  GetLedgerEndRequest,
  GetTransactionsResponse
}
import com.daml.ledger.api.v1.{CommandServiceGrpc, TransactionServiceGrpc}
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.GrpcClientResource
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
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
      port = Port.Dynamic,
      damlPackages = List(testDalf),
      ledgerIdMode = LedgerIdMode.Static(LedgerId(LedgerID)),
      timeProviderType = Some(TimeProviderType.WallClock),
    )

    val channelOwner = for {
      server <- SandboxServer.owner(config)
      channel <- GrpcClientResource.owner(server.port)
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
              Optional.empty[Instant],
              Optional.empty[Duration],
              Optional.empty[Duration],
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
