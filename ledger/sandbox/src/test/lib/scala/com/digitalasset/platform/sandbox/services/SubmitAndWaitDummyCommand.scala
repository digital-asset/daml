// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.platform.sandbox.auth.ServiceCallWithMainActorAuthTests
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitAndWaitDummyCommand extends TestCommands { self: ServiceCallWithMainActorAuthTests =>

  protected def submitAndWait(): Future[Empty] =
    submitAndWait(Option(toHeader(readWriteToken(mainActor))))

  protected def dummySubmitAndWaitRequest: SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyCommands(wrappedLedgerId, s"$serviceCallName-${UUID.randomUUID}", mainActor)
        .update(_.commands.applicationId := serviceCallName, _.commands.party := mainActor)
        .commands)

  private def service(token: Option[String]) =
    stub(CommandServiceGrpc.stub(channel), token)

  protected def submitAndWait(token: Option[String]): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest)

  protected def submitAndWaitForTransaction(token: Option[String]): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest)

  protected def submitAndWaitForTransactionId(token: Option[String]): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest)

  protected def submitAndWaitForTransactionTree(token: Option[String]): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest)

}
