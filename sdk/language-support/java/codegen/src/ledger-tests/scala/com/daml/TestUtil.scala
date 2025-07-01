// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v2.{CommandServiceGrpc, StateServiceGrpc}
import com.daml.ledger.api.v2.StateServiceOuterClass.{
  GetActiveContractsResponse,
  GetLedgerEndRequest,
}
import com.daml.ledger.api.v2.CommandServiceOuterClass.{SubmitAndWaitRequest, SubmitAndWaitResponse}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{codegen => jcg, _}
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.daml.timer.RetryStrategy
import io.grpc.Channel
import org.scalatest.{Assertion, Suite}

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Optional, UUID}
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.jdk.javaapi.OptionConverters
import scala.language.implicitConversions

trait TestLedger extends CantonFixture with SuiteResourceManagementAroundAll {
  self: Suite =>

  override protected lazy val darFiles = List(
    BazelRunfiles.rlocation(Paths.get("language-support/java/codegen/ledger-tests-model.dar"))
  )

  private var client: LedgerClient = _

  override protected def beforeAll(): scala.Unit = {
    implicit def executionContext: ExecutionContext = ExecutionContext.global
    super.beforeAll()
    client = Await.result(defaultLedgerClient(), 10.seconds)
  }

  protected def allocateParty: Future[String] = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    for {
      party <- client.partyManagementClient
        .allocateParty(hint = None, token = None)
        .map(_.party)
      _ <- RetryStrategy.constant(5, 200.milliseconds) { case (_, _) =>
        for {
          res <- client.stateService
            .getConnectedSynchronizers(party = party, token = None)
          _ <-
            if (res.connectedSynchronizers.isEmpty)
              Future.failed(
                new java.util.concurrent.TimeoutException(
                  "Party not allocated on any domains within 1 second"
                )
              )
            else Future.unit
        } yield ()
      }
    } yield party
  }

  def withClient(testCode: Channel => Future[Assertion]): Future[Assertion] = testCode(
    client.channel
  )
}

// TransactionFilter will be removed in 3.4, use EventFormat instead
@nowarn("cat=deprecation")
object TestUtil {

  val LedgerID = "participant0"

  // unfortunately this is needed to help with passing functions to rxjava methods like Flowable#map
  implicit def func2rxfunc[A, B](f: A => B): io.reactivex.functions.Function[A, B] = f(_)
  private def randomId = UUID.randomUUID().toString

  def allTemplates(partyName: String) = new TransactionFilter(
    Map[String, Filter](partyName -> NoFilter.instance).asJava,
    None.toJava,
  )

  def sendCmd(channel: Channel, partyName: String, hasCmds: HasCommands*): SubmitAndWaitResponse = {
    val submission = CommandsSubmission
      .create(randomId, randomId, Optional.empty(), HasCommands.toCommands(hasCmds.asJava))
      .withWorkflowId(randomId)
      .withActAs(partyName)

    CommandServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(40, TimeUnit.SECONDS)
      .submitAndWait(
        SubmitAndWaitRequest
          .newBuilder()
          .setCommands(submission.toProto)
          .build
      )
  }

  def sendCmd(
      channel: Channel,
      actAs: java.util.List[String],
      readAs: java.util.List[String],
      hasCmds: HasCommands*
  ): SubmitAndWaitResponse = {
    val submission = CommandsSubmission
      .create(randomId, randomId, Optional.empty(), HasCommands.toCommands(hasCmds.asJava))
      .withWorkflowId(randomId)
      .withActAs(actAs)
      .withReadAs(readAs)

    CommandServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(40, TimeUnit.SECONDS)
      .submitAndWait(
        SubmitAndWaitRequest
          .newBuilder()
          .setCommands(submission.toProto)
          .build
      )
  }

  def readActiveContractKeys[K, C <: jcg.ContractWithKey[_, _, K]](
      companion: jcg.ContractCompanion.WithKey[C, _, _, K]
  )(channel: Channel, partyName: String): List[Optional[K]] =
    readActiveContracts(companion.fromCreatedEvent)(channel, partyName).map(_.key)

  def readActiveContractPayloads[T <: jcg.DamlRecord[_], C <: jcg.Contract[_, T]](
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
    val stateService = StateServiceGrpc.newBlockingStub(channel)
    val currentEnd = stateService.getLedgerEnd(GetLedgerEndRequest.newBuilder().build()).getOffset
    val txs = stateService.getActiveContracts(
      new GetActiveContractsRequest(
        allTemplates(partyName),
        true,
        currentEnd,
      ).toProto
    )
    val iterable: java.lang.Iterable[GetActiveContractsResponse] = () => txs
    StreamSupport
      .stream(iterable.spliterator(), false)
      .flatMap[CreatedEvent]((r: GetActiveContractsResponse) =>
        data.GetActiveContractsResponse
          .fromProto(r)
          .getContractEntry
          .map(_.getCreatedEvent)
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
