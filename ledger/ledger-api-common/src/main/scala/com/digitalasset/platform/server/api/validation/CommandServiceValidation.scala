// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class CommandServiceValidation(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: String)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable
    with CommandValidations
    with ErrorFactories
    with CommandPayloadValidations {

  protected val logger: Logger = LoggerFactory.getLogger(CommandService.getClass)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    val validation = requirePresence(request.commands, "commands").flatMap(validateCommands)
    validation.fold(Future.failed, _ => service.submitAndWait(request))
  }

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
