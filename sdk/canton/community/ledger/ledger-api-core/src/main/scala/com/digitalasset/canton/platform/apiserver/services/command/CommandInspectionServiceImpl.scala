// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.ledger.api.v1.admin.command_inspection_service.{
  CommandInspectionServiceGrpc,
  CommandState,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandInspectionService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.{CommandProgressTracker, CommandStatus}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiCommandInspectionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class CommandInspectionServiceImpl private (
    tracker: CommandProgressTracker,
    val loggerFactory: NamedLoggerFactory,
) extends CommandInspectionService
    with NamedLogging {

  override def findCommandStatus(
      commandIdPrefix: String,
      state: CommandState,
      limit: Int,
  ): Future[Seq[CommandStatus]] =
    tracker.findCommandStatus(commandIdPrefix, state, limit)
}

private[apiserver] object CommandInspectionServiceImpl {

  def createApiService(
      tracker: CommandProgressTracker,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ApiCommandInspectionService & GrpcApiService = {
    val impl: CommandInspectionService =
      new CommandInspectionServiceImpl(
        tracker,
        loggerFactory,
      )

    new ApiCommandInspectionService(
      impl,
      telemetry,
      loggerFactory,
    ) with GrpcApiService {
      override def bindService(): ServerServiceDefinition =
        CommandInspectionServiceGrpc.bindService(this, executionContext)
    }
  }
}
