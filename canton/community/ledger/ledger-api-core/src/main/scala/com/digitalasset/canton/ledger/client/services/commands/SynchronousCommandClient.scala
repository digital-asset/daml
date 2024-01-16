// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v1.command_service.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.google.protobuf.empty.Empty
import io.grpc.stub.AbstractStub

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class SynchronousCommandClient(service: CommandServiceStub) {

  def submitAndWait(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[Empty] =
    withDeadline(LedgerClient.stub(service, token), timeout).submitAndWait(submitAndWaitRequest)

  def submitAndWaitForTransactionId(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForTransactionId(submitAndWaitRequest)

  def submitAndWaitForTransaction(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForTransactionResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForTransaction(submitAndWaitRequest)

  def submitAndWaitForTransactionTree(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForTransactionTree(submitAndWaitRequest)

  private def withDeadline[S <: AbstractStub[S]](stub: S, timeout: Option[Duration]): S =
    timeout.fold(stub)(timeout => stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS))
}
