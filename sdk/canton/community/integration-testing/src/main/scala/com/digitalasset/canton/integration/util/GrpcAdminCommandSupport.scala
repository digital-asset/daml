// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.syntax.either.*
import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  LocalParticipantReference,
}
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ManagedChannel}

import scala.concurrent.Future

object GrpcAdminCommandSupport {

  implicit class StubAdminCommandOps[SVC <: AbstractStub[SVC]](stub: Channel => SVC) {
    def syncCommand[REQ, RESP](
        syncMethod: SVC => REQ => Future[RESP]
    ): REQ => GrpcAdminCommand[REQ, RESP, RESP] = { request =>
      new GrpcAdminCommand[REQ, RESP, RESP] {
        override type Svc = SVC
        override def createService(channel: ManagedChannel): SVC = stub(channel)
        override protected def submitRequest(service: SVC, request: REQ): Future[RESP] =
          syncMethod(service)(request)
        override protected def createRequest(): Either[String, REQ] = Right(request)
        override protected def handleResponse(response: RESP): Either[String, RESP] = Right(
          response
        )
      }
    }

    def streamingCommand[REQ, RESP](
        streamMethod: SVC => (REQ, StreamObserver[RESP]) => Unit,
        filter: RESP => Boolean = (_: RESP) => true,
    ): REQ => ConsoleEnvironment => Int => GrpcAdminCommand[REQ, Seq[RESP], Seq[RESP]] = {
      request => consoleEnvironment => expected =>
        new GrpcAdminCommand[REQ, Seq[RESP], Seq[RESP]] {
          override type Svc = SVC
          override def createService(channel: ManagedChannel): SVC = stub(channel)
          override protected def submitRequest(service: SVC, request: REQ): Future[Seq[RESP]] =
            GrpcAdminCommand.streamedResponse[REQ, RESP, RESP](
              service = streamMethod(service),
              extract = Seq(_).filter(filter),
              request = request,
              expected = expected,
              timeout = consoleEnvironment.commandTimeouts.ledgerCommand.asFiniteApproximation,
              scheduler = consoleEnvironment.environment.scheduler,
            )
          override protected def createRequest(): Either[String, REQ] = Right(request)
          override protected def handleResponse(response: Seq[RESP]): Either[String, Seq[RESP]] =
            Right(
              response
            )
        }
    }
  }

  implicit class ParticipantReferenceOps(
      participant: LocalParticipantReference
  ) {
    def runLapiAdminCommand[REQ, RESP, RESULT](
        command: GrpcAdminCommand[REQ, RESP, RESULT]
    ): ConsoleCommandResult[RESULT] =
      participant.consoleEnvironment.grpcLedgerCommandRunner.runCommand(
        participant.name,
        command,
        participant.config.clientLedgerApi,
        participant.adminToken,
      )

    def runStreamingLapiAdminCommand[REQ, RESP, RESULT](
        command: ConsoleEnvironment => Int => GrpcAdminCommand[REQ, RESP, RESULT],
        expected: Int = Int.MaxValue,
    ): ConsoleCommandResult[RESULT] =
      participant.consoleEnvironment.grpcLedgerCommandRunner.runCommand(
        participant.name,
        command(participant.consoleEnvironment)(expected),
        participant.config.clientLedgerApi,
        participant.adminToken,
      )
  }

  implicit class AdminCommandOps[T](val consoleCommandResult: ConsoleCommandResult[T])
      extends AnyVal {
    def tryResult: T =
      consoleCommandResult.toEither.valueOr(error => throw new RuntimeException(error))
  }
}

private[integration] object GrpcServices {
  import GrpcAdminCommandSupport.*

  object ReassignmentsService {
    val stub: Channel => CommandSubmissionServiceStub =
      proto.command_submission_service.CommandSubmissionServiceGrpc.stub
    val submit = stub.syncCommand(_.submitReassignment)
  }

  object StateService {
    val stub: Channel => StateServiceStub = proto.state_service.StateServiceGrpc.stub
    val getActiveContracts = stub.streamingCommand(_.getActiveContracts)
    val getConnectedSynchronizers = stub.syncCommand(_.getConnectedSynchronizers)
  }
}
