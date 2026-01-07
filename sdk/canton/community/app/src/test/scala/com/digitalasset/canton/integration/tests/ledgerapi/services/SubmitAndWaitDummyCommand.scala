// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.{
  ServiceCallAuthTests,
  ServiceCallWithMainActorAuthTests,
}
import com.google.protobuf.empty.Empty

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait SubmitAndWaitDummyCommand extends TestCommands with SubmitAndWaitDummyCommandHelpers {
  self: ServiceCallWithMainActorAuthTests =>

  protected def submitAndWaitAsMainActor(
      mainActorId: String
  )(implicit ec: ExecutionContext): Future[Empty] =
    submitAndWait(
      canActAsMainActor.token,
      userId = mainActorActUser,
      party = mainActorId,
    )

}

trait SubmitAndWaitDummyCommandHelpers extends TestCommands {
  self: ServiceCallAuthTests =>

  protected def dummySubmitAndWaitRequest(
      userId: String,
      party: String,
      commandId: Option[String] = None,
  ): SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyCommands(commandId.getOrElse(s"$serviceCallName-${UUID.randomUUID}"), party = party)
        .update(_.commands.userId := userId, _.commands.actAs := Seq(party))
        .commands
    )

  protected def dummySubmitAndWaitForTransactionRequest(
      userId: String,
      party: String,
      commandId: Option[String] = None,
  ): SubmitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(
      commands = dummySubmitAndWaitRequest(userId, party, commandId).commands,
      transactionFormat = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(party -> Filters(Nil)),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      ),
    )

  private def service(token: Option[String]) =
    stub(CommandServiceGrpc.stub(channel), token)

  protected def submitAndWait(
      token: Option[String],
      userId: String,
      party: String,
      commandId: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[Empty] =
    service(token)
      .submitAndWait(dummySubmitAndWaitRequest(userId, party = party, commandId = commandId))
      .map(_ => Empty())

  protected def submitAndWaitForTransaction(
      token: Option[String],
      userId: String,
      party: String,
      commandId: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[Empty] =
    service(token)
      .submitAndWaitForTransaction(
        dummySubmitAndWaitForTransactionRequest(userId, party = party, commandId = commandId)
      )
      .map(_ => Empty())

}
