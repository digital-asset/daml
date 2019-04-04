// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class CommandServiceImpl(response: Future[Empty]) extends CommandService {

  private var lastRequest: Option[SubmitAndWaitRequest] = None

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    this.lastRequest = Some(request)
    response
  }

  def getLastRequest: Option[SubmitAndWaitRequest] = this.lastRequest
}

object CommandServiceImpl {

  def createWithRef(response: Future[Empty])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, CommandServiceImpl) = {
    val serviceImpl = new CommandServiceImpl(response)
    (CommandServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
