// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.io.File
import java.time.{Duration, Instant}
import java.util.Arrays.asList
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Optional, UUID}

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass.GetActiveContractsResponse
import com.daml.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.daml.ledger.api.v1.{ActiveContractsServiceGrpc, CommandServiceGrpc}
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data._
import com.daml.ledger.resources.ResourceContext
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

object TestUtil {

  def testDalf =
    new File(BazelRunfiles.rlocation("language-support/java/codegen/ledger-tests-model.dar"))

  val LedgerID = "ledger-test"

  def withClient(
      testCode: Channel => Assertion
  )(implicit resourceContext: ResourceContext): Future[Assertion] = {
    val config = sandbox.DefaultConfig.copy(
      seeding = Some(Seeding.Weak),
      port = Port.Dynamic,
      damlPackages = List(testDalf),
      ledgerIdMode = LedgerIdMode.Static(LedgerId(LedgerID)),
      timeProviderType = Some(TimeProviderType.WallClock),
    )

    val channelOwner = for {
      server <- SandboxServer.owner(config)
      channel <- GrpcClientResource.owner(server.port)
    } yield channel
    channelOwner.use(channel => Future(testCode(channel))(resourceContext.executionContext))
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
              cmds.asJava,
            )
          )
          .build
      )
  }

  def sendCmd(
      channel: Channel,
      actAs: java.util.List[String],
      readAs: java.util.List[String],
      cmds: Command*
  ): Empty = {
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
              actAs,
              readAs,
              Optional.empty[Instant],
              Optional.empty[Duration],
              Optional.empty[Duration],
              cmds.asJava,
            )
          )
          .build
      )
  }

  def sendCmd(channel: Channel, party: String, cmds: Command*): Empty =
    sendCmd(channel, asList(party), asList[String](), cmds: _*)

  def readActiveContracts[C <: Contract](fromCreatedEvent: CreatedEvent => C)(
      channel: Channel
  ): List[C] = {
    // Relies on ordering of ACS endpoint. This isn’t documented but currently
    // the ledger guarantees this.
    val txService = ActiveContractsServiceGrpc.newBlockingStub(channel)
    val txs = txService.getActiveContracts(
      new GetActiveContractsRequest(
        LedgerID,
        allTemplates,
        true,
      ).toProto
    )
    val iterable: java.lang.Iterable[GetActiveContractsResponse] = () => txs
    StreamSupport
      .stream(iterable.spliterator(), false)
      .flatMap[CreatedEvent]((r: GetActiveContractsResponse) =>
        data.GetActiveContractsResponse
          .fromProto(r)
          .getCreatedEvents
          .stream()
      )
      .map[C]((e: CreatedEvent) => fromCreatedEvent(e))
      .collect(Collectors.toList[C])
      .asScala
      .toList
  }

}
