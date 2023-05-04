// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
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
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.daml.ledger.javaapi.data.{codegen => jcg, _}
import com.daml.lf.integrationtest.CantonFixture
import com.daml.platform.services.time.TimeProviderType
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.{Assertion, Suite}

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Optional, UUID}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters
import scala.language.implicitConversions

trait TestLedger extends CantonFixture {
  self: Suite =>

  protected def darFiles: List[Path] = List(
    BazelRunfiles.rlocation(Paths.get("language-support/java/codegen/ledger-tests-model.dar"))
  )

  protected def devMode: Boolean = false
  protected def nParticipants: Int = 1
  protected def timeProviderType: TimeProviderType = TimeProviderType.Static
  override protected lazy val applicationId = ApplicationId(getClass.getSimpleName)
  override protected def authSecret: Option[String] = None
  override protected def tlsEnable: Boolean = false
  override protected lazy val cantonFixtureDebugMode = true

  protected lazy val damlPackages: List[Path] = List(
    Paths.get(BazelRunfiles.rlocation("language-support/java/codegen/ledger-tests-model.dar"))
  )

  protected val clientConfiguration: LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = TestUtil.LedgerID,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  lazy val severPort = suiteResource.value.head
  lazy val channel = config.channel(severPort)

  protected def allocateParty: Future[String] = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    for {
      client <- LedgerClient(channel, clientConfiguration)
      allocatedParty <- client.partyManagementClient
        .allocateParty(hint = None, displayName = None)
    } yield allocatedParty.party
  }

  def withClient(testCode: Channel => Future[Assertion]): Future[Assertion] = testCode(channel)
}

object TestUtil {

  val LedgerID = "participant0"

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

  def allTemplates(partyName: String) = new FiltersByParty(
    Map[String, Filter](partyName -> NoFilter.instance).asJava
  )

  def sendCmd(channel: Channel, partyName: String, hasCmds: HasCommands*): Empty = {
    val submission = CommandsSubmission
      .create(randomId, randomId, HasCommands.toCommands(hasCmds.asJava))
      .withWorkflowId(randomId)
      .withActAs(partyName)

    CommandServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(40, TimeUnit.SECONDS)
      .submitAndWait(
        SubmitAndWaitRequest
          .newBuilder()
          .setCommands(
            SubmitCommandsRequest.toProto(
              LedgerID,
              submission,
            )
          )
          .build
      )
  }

  def sendCmd(
      channel: Channel,
      actAs: java.util.List[String],
      readAs: java.util.List[String],
      hasCmds: HasCommands*
  ): Empty = {
    val submission = CommandsSubmission
      .create(randomId, randomId, HasCommands.toCommands(hasCmds.asJava))
      .withWorkflowId(randomId)
      .withActAs(actAs)
      .withReadAs(readAs)

    CommandServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(40, TimeUnit.SECONDS)
      .submitAndWait(
        SubmitAndWaitRequest
          .newBuilder()
          .setCommands(
            SubmitCommandsRequest.toProto(
              LedgerID,
              submission,
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
