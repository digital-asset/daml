// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.error.{ErrorCategory, ErrorClass, ErrorCode}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.commands.Commands
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.ledger.client.services.commands.SynchronousCommandClientV1
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto
import org.scalatest.*
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.Duration

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class CommandSubmitterWithRetryTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with PekkoBeforeAndAfterAll {
  private val maxRetries = 10
  private val timeout = Duration.fromNanos(1337L)
  private val commands = Commands(
    ledgerId = "ledgerId",
    workflowId = "workflowId",
    applicationId = "applicationId",
    commandId = "commandId",
    party = "party",
    commands = Nil,
  )

  final class Fixture {
    private var sut: CommandSubmitterWithRetry = _

    def runTest(
        expectedCommands: Commands,
        result: Future[Either[Status, String]],
    )(
        test: (CommandSubmitterWithRetry, SynchronousCommandClientV1) => Future[Assertion]
    ): Future[Assertion] = {
      val synchronousCommandClient = mock[SynchronousCommandClientV1]
      val request = SubmitAndWaitRequest(Some(expectedCommands))
      when(synchronousCommandClient.submitAndWaitForTransactionId(request, None, Some(timeout)))
        .thenReturn(
          result.flatMap(
            _.fold(
              nonZeroStatus =>
                Future.failed(
                  StatusProto.toStatusRuntimeException(Status.toJavaProto(nonZeroStatus))
                ),
              tId => Future.successful(SubmitAndWaitForTransactionIdResponse(transactionId = tId)),
            )
          )
        )
      sut = new CommandSubmitterWithRetry(
        10,
        synchronousCommandClient,
        FutureSupervisor.Noop,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

      test(sut, synchronousCommandClient)
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
      f.runTest(commands, Future.successful(Right(transactionId))) { (sut, _) =>
        for {
          result <- sut.submitCommands(commands, timeout)
        } yield result shouldBe CommandResult.Success(transactionId)
      }
    }

    "complete with failure" in { f =>
      val errorStatus = Status(Code.FAILED_PRECONDITION.value, "oh no man", Nil)
      f.runTest(commands, Future.successful(Left(errorStatus))) { (sut, _) =>
        for {
          result <- sut.submitCommands(commands, timeout)
        } yield result shouldBe CommandResult.Failed(commands.commandId, errorStatus)
      }
    }

    "retry on timeouts at most given maxRetries" in { f =>
      val code =
        new ErrorCode(id = "TIMEOUT", ErrorCategory.ContentionOnSharedResources)(
          new ErrorClass(Nil)
        ) {
          override def errorConveyanceDocString: Option[String] = None
        }
      val errorStatus = Status(Code.ABORTED.value, code.toMsg(s"now try that", None), Nil)

      f.runTest(commands, Future.successful(Left(errorStatus))) { (sut, commandClient) =>
        loggerFactory.suppressWarningsAndErrors {
          for {
            result <- sut.submitCommands(commands, timeout)
          } yield {
            verify(commandClient, times(maxRetries + 1))
              .submitAndWaitForTransactionId(
                SubmitAndWaitRequest(Some(commands)),
                None,
                Some(timeout),
              )
            result shouldBe CommandResult.MaxRetriesReached(commands.commandId, errorStatus)
          }
        }
      }
    }

    "Gracefully reject commands submitted after closing" in { f =>
      f.runTest(commands, Future.never) { (sut, _) =>
        sut.close()
        sut.submitCommands(commands, timeout).map(_ shouldBe CommandResult.AbortedDueToShutdown)
      }
    }

    "Gracefully reject pending commands when closing" in { f =>
      f.runTest(commands, Future.never) { (sut, _) =>
        val result =
          sut.submitCommands(commands, timeout).map(_ shouldBe CommandResult.AbortedDueToShutdown)
        sut.close()
        result
      }
    }
  }
}
