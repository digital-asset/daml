// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class CommandService(channel: Channel, authorizationToken: Option[String]) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: CommandServiceGrpc.CommandServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(CommandServiceGrpc.stub(channel))

  def submitAndWait(
      commands: Commands
  )(implicit ec: ExecutionContext): Future[SubmitAndWaitResponse] =
    service
      .submitAndWait(new SubmitAndWaitRequest(Some(commands)))
      .recoverWith { case NonFatal(ex) =>
        Future.failed {
          logger.error(s"Command submission error. Details: ${ex.getLocalizedMessage}", ex)
          ex
        }
      }
}
