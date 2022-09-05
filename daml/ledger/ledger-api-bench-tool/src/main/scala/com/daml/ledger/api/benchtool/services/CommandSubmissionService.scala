// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.command_submission_service._
import com.daml.ledger.api.v1.commands.Commands
import com.google.protobuf.empty.Empty
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class CommandSubmissionService(channel: Channel, authorizationToken: Option[String]) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val service: CommandSubmissionServiceGrpc.CommandSubmissionServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      CommandSubmissionServiceGrpc.stub(channel)
    )

  def submit(commands: Commands)(implicit ec: ExecutionContext): Future[Empty] =
    service
      .submit(new SubmitRequest(Some(commands)))
      .recoverWith { case NonFatal(ex) =>
        Future.failed {
          logger.error(s"Command submission error. Details: ${ex.getLocalizedMessage}", ex)
          ex
        }
      }

}
