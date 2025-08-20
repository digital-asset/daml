// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import cats.syntax.either.*
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateServiceStub
import com.daml.ledger.api.v2.update_service.{GetUpdatesRequest, UpdateServiceGrpc}
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.UpdateWrapper
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ConsoleCommandResult
import com.digitalasset.canton.damltests.java.simplecontractwithpayload.SimpleContractWithPayload
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.{FullSuppression, LoggerNameContains}
import com.digitalasset.canton.util.{FutureInstances, MonadUtil}
import com.digitalasset.canton.{LfPartyId, config}
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Random, Try}

class LedgerApiStreamingTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  def updateFormat(party: LfPartyId) = UpdateFormat(
    Some(
      TransactionFormat(
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(party -> Filters(Nil)),
            filtersForAnyParty = None,
            verbose = false,
          )
        ),
        transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
      )
    ),
    None,
    None,
  )

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  "test various stream closure scenarios and verify closure in logs, akka stream, gRPC" in {
    implicit env: TestConsoleEnvironment =>
      import env.*

      val suppressionRules = FullSuppression && (
        LoggerNameContains("ApiRequestLogger") ||
          LoggerNameContains("ApiUpdateService")
      )
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonTestsPath)
      val initialLedgerEnd = participant1.ledger_api.state.end()

      logger.info("empty stream should close cleanly from the server side after fully emitted")
      loggerFactory.suppress(suppressionRules) {
        participant1.ledger_api.updates.transactions(
          partyIds = Set(participant1.adminParty),
          completeAfter = 1,
          endOffsetInclusive = Some(initialLedgerEnd),
        )
        assertLogs(
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("received a message GetUpdatesRequest(")
          ),
          _.debugMessage should include("Streaming to gRPC client started"),
          _.debugMessage should include("Streaming to gRPC client finished"),
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("completed")
          ),
        )
      }

      logger.info("empty tailing stream should close with cancel from the client side")
      loggerFactory.suppress(suppressionRules) {
        participant1.ledger_api.updates.transactions(
          partyIds = Set(participant1.adminParty),
          completeAfter = 1,
          endOffsetInclusive = None,
          timeout = config.NonNegativeDuration(FiniteDuration(1, "second")),
        )
        assertLogs(
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("received a message GetUpdatesRequest(")
          ),
          _.debugMessage should include("Streaming to gRPC client started"),
          _.infoMessage should (
            include("UpdateService/GetUpdates") and
              include("cancelled")
          ),
          _.debugMessage should include("Streaming to gRPC client finished"),
        )
      }

      createContract()
      val oneTxLedgerEnd = participant1.ledger_api.state.end()

      logger.info("single-element stream should close cleanly from server ide after fully emitted")
      loggerFactory.suppress(suppressionRules) {
        participant1.ledger_api.updates
          .transactions(
            partyIds = Set(participant1.adminParty),
            completeAfter = 2,
            endOffsetInclusive = Some(oneTxLedgerEnd),
          )
          .size shouldBe 1
        assertLogs(
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("received a message GetUpdatesRequest(")
          ),
          _.debugMessage should include("Streaming to gRPC client started"),
          _.debugMessage should include("Streaming to gRPC client finished"),
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("completed")
          ),
        )
      }

      logger.info("single element tailing stream should close with cancel from the client side")
      loggerFactory.suppress(suppressionRules) {
        participant1.ledger_api.updates.transactions(
          partyIds = Set(participant1.adminParty),
          completeAfter = 2,
          endOffsetInclusive = None,
          timeout = config.NonNegativeDuration(FiniteDuration(1, "second")),
        )
        assertLogs(
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("received a message GetUpdatesRequest(")
          ),
          _.debugMessage should include("Streaming to gRPC client started"),
          _.infoMessage should (
            include("UpdateService/GetUpdates") and
              include("cancelled")
          ),
          _.debugMessage should include("Streaming to gRPC client finished"),
        )
      }

      // Please note this ingestion of test contracts with parallelism 10 took ~11 seconds in local tests
      // (for comparison: the total test run took ~40 seconds)
      // We need volume for the test contracts, so they occupy volume in the update streams,
      // because the client side of gRPC has a buffer, which need to filled even if we block the stream after
      // the first element seen in the TestStreamObserver.
      // Please note: the goal here is to fill the client side buffer, AND fill the server side buffer too, for
      // some following tests.
      logger.info(
        "ingesting 199 more transactions"
      )
      MonadUtil
        .parTraverseWithLimit(10)(1 to 199)(_ => Future(createContract()))(
          FutureInstances.parallelFuture
        )
        .futureValue(Timeout(Span.convertDurationToSpan(FiniteDuration(1, "minute"))))

      val twoHundredTxLedgerEnd = participant1.ledger_api.state.end()

      logger.info(
        "multi element stream emitted successfully (verifying tx fixture and TestStreamObserver)"
      )
      loggerFactory
        .suppress(suppressionRules) {
          val testStreamObserver = new TestStreamObserver[UpdateWrapper]()
          val closeable = participant1.ledger_api.updates.subscribe_updates(
            observer = testStreamObserver,
            updateFormat = updateFormat(participant1.adminParty.toLf),
            endOffsetInclusive = Some(twoHundredTxLedgerEnd),
          )
          testStreamObserver.completion.futureValue
          testStreamObserver.responseCount.get shouldBe 200
          assertLogs(
            _.debugMessage should (
              include("UpdateService/GetUpdates") and
                include("received a message GetUpdatesRequest(")
            ),
            _.debugMessage should include("Streaming to gRPC client started"),
            _.debugMessage should include("Streaming to gRPC client finished"),
            _.debugMessage should (
              include("UpdateService/GetUpdates") and
                include("completed")
            ),
          )
          closeable
        }
        .close()

      logger.info("multi element stream closed immediately from the client side")
      loggerFactory.suppress(suppressionRules) {
        val testStreamObserver = new TestStreamObserver[UpdateWrapper]()
        val closeable = participant1.ledger_api.updates.subscribe_updates(
          observer = testStreamObserver,
          updateFormat = updateFormat(participant1.adminParty.toLf),
          endOffsetInclusive = Some(twoHundredTxLedgerEnd),
        )
        closeable.close()
        testStreamObserver.completion.failed.futureValue // cancellation fails the stream observer
        testStreamObserver.responseCount.get should be < 10 // tolerance needed, if main test runner thread is slow, results might appear before closure
        assertLogs(
          _.infoMessage should (
            include("UpdateService/GetUpdates") and
              include("cancelled")
          )
        )
      }

      logger.info(
        "multi element stream closed immediately from the client side, with daml gRPC client tooling"
      )
      val completeConcoleCommandPromise = Promise[Unit]()
      val consoleCommandDoneF: Future[ConsoleCommandResult[Unit]] =
        loggerFactory.suppress(suppressionRules) {
          val killSwitchPromise = Promise[UniqueKillSwitch]()
          val consoleCommandCompletedF: Future[ConsoleCommandResult[Unit]] = Future {
            participant1.consoleEnvironment.grpcLedgerCommandRunner.runCommand(
              instanceName = participant1.name,
              command = new GrpcAdminCommand[GetUpdatesRequest, Unit, Unit] {
                override type Svc = UpdateServiceStub

                override def createService(channel: ManagedChannel): UpdateServiceStub =
                  UpdateServiceGrpc.stub(channel)

                override protected def submitRequest(
                    service: UpdateServiceStub,
                    request: GetUpdatesRequest,
                ): Future[Unit] = {
                  val (killSwitch, doneF) = ClientAdapter
                    .serverStreaming(request, service.getUpdates)
                    .viaMat(KillSwitches.single)(Keep.right)
                    .toMat(Sink.ignore)(Keep.both)
                    .run()
                  killSwitchPromise.success(killSwitch)
                  // the admin command execution finished after the source is completed
                  doneF
                    .flatMap(_ => completeConcoleCommandPromise.future)
                }

                override protected def createRequest(): Either[String, GetUpdatesRequest] = Right(
                  GetUpdatesRequest(
                    beginExclusive = 0L,
                    endInclusive = Some(twoHundredTxLedgerEnd),
                    filter = None,
                    verbose = false,
                    updateFormat = Some(
                      UpdateFormat(
                        includeTransactions = Some(
                          TransactionFormat(
                            eventFormat = Some(
                              EventFormat(
                                filtersByParty = Map(
                                  participant1.adminParty.toLf -> Filters(Nil)
                                ),
                                filtersForAnyParty = None,
                                verbose = false,
                              )
                            ),
                            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                          )
                        ),
                        includeReassignments = None,
                        includeTopologyEvents = None,
                      )
                    ),
                  )
                )

                override protected def handleResponse(
                    response: Unit
                ): Either[String, Unit] = Right(response)
              },
              clientConfig = participant1.config.clientLedgerApi,
              token = participant1.adminToken,
            )
          }
          // cancel the streaming from the pekko-stream side
          killSwitchPromise.future.futureValue.shutdown()
          assertLogs(
            _.infoMessage should (
              include("UpdateService/GetUpdates") and
                include("cancelled")
            )
          )
          consoleCommandCompletedF
        }
      // complete the console command only after all assertions in the logs met (so accidental cleanup of channels by console command runner not interferes with the result)
      completeConcoleCommandPromise.success(())
      // waiting for the console command to finish successfully
      consoleCommandDoneF.futureValue.toEither shouldBe Either.unit

      logger.info(
        "multi element stream closed after 1 message is emitted to observer which is followed by 5 seconds of delay (letting the server side akka buffer being filled)"
      )
      loggerFactory.suppress(suppressionRules) {
        val testStreamObserver = new TestStreamObserver[UpdateWrapper](1, 5000)
        val closeable = participant1.ledger_api.updates.subscribe_updates(
          observer = testStreamObserver,
          updateFormat = updateFormat(participant1.adminParty.toLf),
          endOffsetInclusive = Some(twoHundredTxLedgerEnd),
        )
        testStreamObserver.afterGatedAndDelayed.futureValue
        closeable.close()
        testStreamObserver.continueAfterGated()
        eventually()(
          testStreamObserver.responseCount.get should (
            be(1) or // in case gRPC termination comes ahead of the 2nd element's processing
              be(2) // in case gRPC termination comes during the 2nd element's processing (the continueAfterGated call after close makes sure no more elements should be processed)
          )
        )
        testStreamObserver.completion.failed.futureValue // cancellation fails the stream observer
        assertLogs(
          _.debugMessage should (
            include("UpdateService/GetUpdates") and
              include("received a message GetUpdatesRequest(")
          ),
          _.debugMessage should include("Streaming to gRPC client started"),
          _.infoMessage should (
            include("UpdateService/GetUpdates") and
              include("cancelled")
          ),
          _.debugMessage should include("Streaming to gRPC client finished"),
        )
      }
  }

  /** Asserting on supressed log messages in order, allowing not mached log messages in between. One
    * assertion relates to one log message. All assertions need to be met, otherwise the test will
    * fail with failure stating how many assertions were met, and with the full sequence of
    * suppressed logs.
    */
  private def assertLogs(
      assertions: (LogEntry => Assertion)*
  )(implicit pos: source.Position): Unit =
    eventually() {
      val allLogEntries = loggerFactory.fetchRecordedLogEntries.toList
      @tailrec
      def assertAllAssertionsFoundInSequence(
          asserts: List[(LogEntry => Assertion)],
          logEntries: List[LogEntry],
      ): Unit =
        (asserts, logEntries) match {
          case (Nil, _) => () // all good, all assertions have been found
          case (assert :: restAssert, logEntry :: restLogEntries) =>
            if (Try(assert(logEntry)).isSuccess)
              assertAllAssertionsFoundInSequence(restAssert, restLogEntries)
            else assertAllAssertionsFoundInSequence(asserts, restLogEntries)
          case (unMetAssertions, Nil) =>
            fail(
              s"${unMetAssertions.size} assertions (from ${assertions.size}) could not met. All log entries:\n ${allLogEntries.map(_.toString).mkString("\n")}"
            )
        }
      assertAllAssertionsFoundInSequence(
        assertions.toList,
        allLogEntries,
      )
    }

  /** Creating a test contract synchronously. For testing purposes this will create a contract of
    * ~10KB strong.
    */
  private def createContract()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val partyId = participant1.adminParty
    val createCmd = new SimpleContractWithPayload(
      partyId.toProtoPrimitive,
      Random.alphanumeric.take(10000).mkString,
    ).create.commands.asScala.toSeq
    participant1.ledger_api.javaapi.commands
      .submit(
        Seq(partyId),
        createCmd,
        commandId = s"createContract-${UUID.randomUUID()}",
        workflowId = Random.alphanumeric.take(255).mkString,
        submissionId = Random.alphanumeric.take(255).mkString,
      )
  }

  /** The purpose of this test observer to excert temporarily backpressure on the related gRCP
    * stream from the client side.
    *
    * Please note that even though this will gate reception of messages as instructed, server side
    * gRCP streaming machinery might go ahead regardless, because of various buffers between the
    * server side publisher and the client side subscriber.
    *
    * This stream observer goes ahead until gateAfterMessages were received, and then it holds
    * reception first waiting for delayAfterGatedMillis milliseconds, then completes the
    * afterGatedAndDelayed Future, then holds recpetion until continueAfterGated signal, then keeps
    * receiving, and completing.
    *
    * This should be used in the following sequence to ensure graceful/proper underlying gRPC
    * streaming functionality.
    *   1. Start related streaming
    *   1. Wait for afterGatedAndDelayed Future
    *   1. Execute continueAfterGated() as convenient, so the stream can continue gracefully
    *   1. Wait for completion Future
    *
    * @param gateAfterMessages
    *   temporarily stop processing after this much time the onNext was called
    * @param delayAfterGatedMillis
    *   delay message processing for this long time, before signalling afterGatedAndDelayed
    */
  private class TestStreamObserver[RespT](
      gateAfterMessages: Int = Int.MaxValue,
      delayAfterGatedMillis: Long = 0,
  ) extends StreamObserver[RespT] {

    private val completionP: Promise[Unit] = Promise[Unit]()
    val completion: Future[Unit] = completionP.future

    private val afterGatedAndDelayedP: Promise[Unit] = Promise[Unit]()
    val afterGatedAndDelayed: Future[Unit] = afterGatedAndDelayedP.future

    private val continueAfterGatedP: Promise[Unit] = Promise[Unit]()

    val responseCount: AtomicInteger = new AtomicInteger()

    override def onNext(value: RespT): Unit = {
      val messageCount = responseCount.incrementAndGet()
      if (messageCount == gateAfterMessages) gate()
      if (messageCount > gateAfterMessages)
        Await.result(continueAfterGatedP.future, FiniteDuration(1, "hour"))
    }

    override def onError(t: Throwable): Unit = {
      val _ = completionP.tryFailure(t)
    }

    override def onCompleted(): Unit = {
      val _ = completionP.trySuccess(())
    }

    def continueAfterGated(): Unit = continueAfterGatedP.trySuccess(())

    private def gate(): Unit = {
      Threading.sleep(delayAfterGatedMillis)
      afterGatedAndDelayedP.trySuccess(())
    }

    assert(gateAfterMessages > 0)
  }
}
