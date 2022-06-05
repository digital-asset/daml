// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass.GetActiveContractsResponse
import com.daml.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.daml.ledger.api.v1.{ActiveContractsServiceGrpc, CommandServiceGrpc}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{codegen => jcg, _}
import com.daml.ledger.sandbox.SandboxOnXForTest.{
  ApiServerConfig,
  Default,
  DevEngineConfig,
  singleParticipant,
}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.{Assertion, Suite}

import java.io.File
import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Optional, UUID}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters
import scala.language.implicitConversions

trait SandboxTestLedger extends SandboxFixture {
  self: Suite =>

  protected val damlPackages: List[File] = List(
    new File(BazelRunfiles.rlocation("language-support/java/codegen/ledger-tests-model.dar"))
  )

  override protected def packageFiles: List[File] = damlPackages

  override def config =
    Default.copy(
      ledgerId = TestUtil.LedgerID,
      engine = DevEngineConfig,
      participants = singleParticipant(
        ApiServerConfig.copy(
          timeProviderType = TimeProviderType.Static,
          seeding = Seeding.Weak,
        )
      ),
    )

  protected val ClientConfiguration: LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = TestUtil.LedgerID,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  protected def allocateParty: Future[String] = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    for {
      client <- LedgerClient(channel, ClientConfiguration)
      allocatedParty <- client.partyManagementClient
        .allocateParty(hint = None, displayName = None)
    } yield allocatedParty.party
  }

  def withClient(testCode: Channel => Future[Assertion]): Future[Assertion] = testCode(channel)
}

object TestUtil {

  val LedgerID = "ledger-test"

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

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

  def readActiveContractKeys[K, C <: jcg.ContractWithKey[_, _, K]](
      companion: jcg.ContractCompanion.WithKey[C, _, _, K]
  )(channel: Channel, partyName: String): List[Optional[K]] =
    readActiveContracts(companion.fromCreatedEvent)(channel, partyName).map(_.key)

  def readActiveContractPayloads[T, C <: jcg.Contract[_, T]](
      companion: jcg.ContractCompanion[C, _, T]
  )(channel: Channel, partyName: String): List[T] =
    readActiveContracts(companion.fromCreatedEvent)(channel, partyName).map(_.data)

  def readActiveContracts[C <: Contract](fromCreatedEvent: CreatedEvent => C)(
      channel: Channel,
      partyName: String,
  ): List[C] =
    readActiveContractsSafe(PartialFunction.fromFunction(fromCreatedEvent))(channel, partyName)

  def readActiveContractsSafe[C <: Contract](fromCreatedEvent: PartialFunction[CreatedEvent, C])(
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
      .flatMap { createdEvent =>
        val res = fromCreatedEvent.lift(createdEvent)
        OptionConverters
          .toJava(res)
          .stream(): java.util.stream.Stream[C]
      }
      .collect(Collectors.toList[C])
      .asScala
      .toList
  }

}
