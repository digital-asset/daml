// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.io.File
import java.time.{Duration, Instant}
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
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.{Assertion, Suite}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

trait SandboxFixture extends SandboxNextFixture {
  self: Suite =>

  protected val damlPackages: List[File] = List(
    new File(BazelRunfiles.rlocation("language-support/java/codegen/ledger-tests-model.dar"))
  )
  protected val ledgerIdMode: LedgerIdMode =
    LedgerIdMode.Static(LedgerId(TestUtil.LedgerID))
  protected override def config: SandboxConfig = SandboxConfig.defaultConfig.copy(
    port = Port.Dynamic,
    ledgerIdMode = ledgerIdMode,
    damlPackages = damlPackages,
    timeProviderType = Some(TimeProviderType.Static),
    engineMode = SandboxConfig.EngineMode.Dev,
    seeding = Some(Seeding.Weak),
  )

  def withClient(
      testCode: Channel => Assertion
  )(implicit resourceContext: ResourceContext): Future[Assertion] = {
    Future(testCode(channel))(resourceContext.executionContext)
  }
}

object TestUtil {

  val LedgerID = "ledger-test"

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

  def uniqueId(): String = UUID.randomUUID.toString

  protected[daml] def getUniqueParty(name: String): String = s"${name}_${uniqueId()}"

  val Alice = "Alice"
  val Bob = "Bob"
  val Charlie = "Charlie"
  def allTemplates(partyName: String) = new FiltersByParty(
    Map[String, Filter](partyName -> NoFilter.instance).asJava
  )

  def sendCmd(channel: Channel, partyName: String, cmds: Command*): Empty = {
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
              partyName,
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

  def readActiveContracts[C <: Contract](fromCreatedEvent: CreatedEvent => C)(
      channel: Channel,
      partyName: String,
  ): List[C] = {
    // Relies on ordering of ACS endpoint. This isn’t documented but currently
    // the ledger guarantees this.
    val txService = ActiveContractsServiceGrpc.newBlockingStub(channel)
    val txs = txService.getActiveContracts(
      new GetActiveContractsRequest(
        LedgerID,
        allTemplates(partyName),
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
