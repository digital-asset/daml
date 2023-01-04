// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.platform.sandbox.auth.{ServiceCallAuthTests, ServiceCallWithMainActorAuthTests}
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitAndWaitDummyCommand extends TestCommands with SubmitAndWaitDummyCommandHelpers {
  self: ServiceCallWithMainActorAuthTests =>

  protected def submitAndWaitAsMainActor(): Future[Empty] =
    submitAndWait(
      Option(toHeader(readWriteToken(mainActor))),
      applicationId = serviceCallName,
      party = mainActor,
    )

}

trait SubmitAndWaitDummyCommandHelpers extends TestCommands {
  self: ServiceCallAuthTests =>

  protected def dummySubmitAndWaitRequest(
      applicationId: String,
      party: String,
  ): SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyCommands(wrappedLedgerId, s"$serviceCallName-${UUID.randomUUID}", party = party)
        .update(_.commands.applicationId := applicationId, _.commands.party := party)
        .commands
    )

  private def service(token: Option[String]) =
    stub(CommandServiceGrpc.stub(channel), token)

  protected def submitAndWait(
      token: Option[String],
      applicationId: String = serviceCallName,
      party: String,
  ): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest(applicationId, party = party))

  protected def submitAndWaitForTransaction(
      token: Option[String],
      applicationId: String = serviceCallName,
      party: String,
  ): Future[Empty] =
    service(token)
      .submitAndWaitForTransaction(dummySubmitAndWaitRequest(applicationId, party = party))
      .map(_ => Empty())

  protected def submitAndWaitForTransactionId(
      token: Option[String],
      applicationId: String = serviceCallName,
      party: String,
  ): Future[Empty] =
    service(token)
      .submitAndWaitForTransactionId(dummySubmitAndWaitRequest(applicationId, party = party))
      .map(_ => Empty())

  protected def submitAndWaitForTransactionTree(
      token: Option[String],
      applicationId: String = serviceCallName,
      party: String,
  ): Future[Empty] =
    service(token)
      .submitAndWaitForTransactionTree(dummySubmitAndWaitRequest(applicationId, party = party))
      .map(_ => Empty())

}
