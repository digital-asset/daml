// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.command_service.SubmitAndWaitForUpdateIdResponse
import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.util.SingleUseCell
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import org.scalatest.*
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class CommandSubmitterWithRetryTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with PekkoBeforeAndAfterAll {

  private val timeout = 5.seconds
  private val commands = Commands(
    workflowId = "workflowId",
    applicationId = "applicationId",
    commandId = "commandId",
    actAs = Seq("party"),
    commands = Nil,
  )

  final class Fixture {

    private var sut: CommandSubmitterWithRetry = _

    def runTest(
        expectedCommands: Commands,
        result: => Future[Either[Status, String]],
    )(
        test: (CommandSubmitterWithRetry, CommandServiceClient, SimClock) => Future[Assertion]
    ): Future[Assertion] = {
      val synchronousCommandClient = mock[CommandServiceClient]
      val simClock = new SimClock(loggerFactory = loggerFactory)
      when(synchronousCommandClient.submitAndWaitForUpdateId(expectedCommands, Some(timeout)))
        .thenAnswer(
          result.map(_.map(updateId => SubmitAndWaitForUpdateIdResponse(updateId = updateId)))
        )
      sut = new CommandSubmitterWithRetry(
        synchronousCommandClient,
        simClock,
        FutureSupervisor.Noop,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

      test(sut, synchronousCommandClient, simClock)
    }

    def close(): Unit = sut.close()
  }

  override type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val fixture = new Fixture

    complete {
      super.withFixture(test.toNoArgAsyncTest(fixture))
    } lastly {
      fixture.close()
    }
  }

  "CommandSubmitterWithRetry" should {
    val transactionId = "txId"
    "complete successfully" in { f =>
      f.runTest(commands, Future.successful(Right(transactionId))) { (sut, _, _) =>
        for {
          result <- sut.submitCommands(commands, timeout)
        } yield result shouldBe CommandResult.Success(transactionId)
      }
    }

    "complete with failure" in { f =>
      val errorStatus = Status(Code.FAILED_PRECONDITION.value, "oh no man", Nil)
      f.runTest(commands, Future.successful(Left(errorStatus))) { (sut, _, _) =>
        for {
          result <- sut.submitCommands(commands, timeout)
        } yield result shouldBe CommandResult.Failed(commands.commandId, errorStatus)
      }
    }

    "retry on retryable until expired" in { f =>
      val errorCode =
        new ErrorCode(id = "CONTENTION", ErrorCategory.ContentionOnSharedResources)(
          new ErrorClass(Nil)
        ) {
          override def errorConveyanceDocString: Option[String] = None
        }
      val errorStatus = CommandServiceClient
        .statusFromThrowable(
          ErrorCode
            .asGrpcError(new BaseError() {
              override def code: ErrorCode = errorCode
              override def cause: String = "now try that"
            })
        )
        .valueOrFail("Could not create error status")

      val responded = new SingleUseCell[SimClock]()

      f.runTest(
        commands, {
          responded.get.foreach(_.advance(1.second.toJava)) // advance clock in every response
          Future.successful(Left(errorStatus))
        },
      ) { (sut, commandClient, clock) =>
        responded.putIfAbsent(clock)
        loggerFactory.suppressWarningsAndErrors {
          for {
            result <- sut.submitCommands(commands, timeout)
          } yield {
            verify(commandClient, times(4))
              .submitAndWaitForUpdateId(
                commands,
                Some(timeout),
              )
            result shouldBe CommandResult.TimeoutReached(commands.commandId, errorStatus)
          }
        }
      }
    }

    "Gracefully reject commands submitted after closing" in { f =>
      f.runTest(commands, Future.never) { (sut, _, _) =>
        sut.close()
        sut.submitCommands(commands, timeout).map(_ shouldBe CommandResult.AbortedDueToShutdown)
      }
    }

    "Gracefully reject pending commands when closing" in { f =>
      f.runTest(commands, Future.never) { (sut, _, _) =>
        val result =
          sut.submitCommands(commands, timeout).map(_ shouldBe CommandResult.AbortedDueToShutdown)
        sut.close()
        result
      }
    }
  }
}
