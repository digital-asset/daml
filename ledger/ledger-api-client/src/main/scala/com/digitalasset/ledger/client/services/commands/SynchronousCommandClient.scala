// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.client.LedgerClient
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

class SynchronousCommandClient(service: CommandServiceStub) {

  def submitAndWait(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
  ): Future[Empty] =
    LedgerClient.stub(service, token).submitAndWait(submitAndWaitRequest)

  def submitAndWaitForTransactionId(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    LedgerClient.stub(service, token).submitAndWaitForTransactionId(submitAndWaitRequest)

  def submitAndWaitForTransaction(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
  ): Future[SubmitAndWaitForTransactionResponse] =
    LedgerClient.stub(service, token).submitAndWaitForTransaction(submitAndWaitRequest)

  def submitAndWaitForTransactionTree(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    LedgerClient.stub(service, token).submitAndWaitForTransactionTree(submitAndWaitRequest)

}
