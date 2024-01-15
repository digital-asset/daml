// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.{ActiveContractsServiceGrpc, CommandServiceGrpc}
import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass.GetActiveContractsResponse
import com.daml.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.{codegen => jcg, Unit => _, _}
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.google.protobuf.Empty
import io.grpc.Channel
import org.scalatest.{Assertion, Suite}

import java.nio.file.Paths
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.stream.{Collectors, StreamSupport}
import java.util.{Optional, UUID}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters
import scala.language.implicitConversions

trait TestLedger extends CantonFixture with SuiteResourceManagementAroundAll {
  self: Suite =>

  /** A scheduled service executor used by [[sleep]]. */
  private val scheduledExecutorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  /** Returns a future that completes after the provided delay. */
  private def sleep(delay: FiniteDuration): Future[Unit] = {
    val promise = Promise[Unit]()
    val _ = scheduledExecutorService.schedule(() => promise.success(()), delay.length, delay.unit)
    promise.future
  }

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
      partyDetails <- client.partyManagementClient.allocateParty(hint = None, displayName = None)
      // TODO(https://github.com/DACH-NY/canton/issues/16401): remove once we have a better way of
      //   synchronizing after a party allocation.
      _ <- sleep(250.millis)
    } yield partyDetails.party
  }

  def withClient(testCode: Channel => Future[Assertion]): Future[Assertion] = testCode(
    client.channel
  )
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
