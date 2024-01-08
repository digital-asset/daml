// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient.statusFromThrowable
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandServiceClient(service: CommandServiceStub)(implicit
    executionContext: ExecutionContext
) {

  def submitAndWait(commands: Commands): Future[Either[Status, Unit]] = {
    service
      .submitAndWait(SubmitAndWaitRequest(commands = Some(commands)))
      .transformWith {
        case Success(_) => Future.successful(Right(()))
        case Failure(exception) =>
          statusFromThrowable(exception) match {
            case Some(value) => Future.successful(Left(value))
            case None => Future.failed(exception)
          }
      }
  }

}

object CommandServiceClient {
  private def statusFromThrowable(throwable: Throwable): Option[Status] =
    Option(StatusProto.fromThrowable(throwable)).map(Status.fromJavaProto)

}
