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
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallAuthTests
import com.google.protobuf.empty.Empty

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait SubmitAndWaitMultiPartyDummyCommand extends TestCommands { self: ServiceCallAuthTests =>

  protected def dummySubmitAndWaitRequest(
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  ): SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyMultiPartyCommands(
        s"${serviceCallName.filter(_.isLetterOrDigit)}-${UUID.randomUUID}",
        actAs,
        readAs,
      )
        .update(_.commands.userId := userId)
        .commands
    )

  protected def dummySubmitAndWaitForTransactionRequest(
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  ): SubmitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(
      commands = dummySubmitAndWaitRequest(actAs, readAs, userId).commands,
      transactionFormat = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = actAs.map(_ -> Filters(Nil)).toMap,
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

  protected def submitAndWaitForTransaction(
      token: Option[String],
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  )(implicit ec: ExecutionContext): Future[Empty] =
    service(token)
      .submitAndWaitForTransaction(
        dummySubmitAndWaitForTransactionRequest(actAs, readAs, userId)
      )
      .map(_ => Empty())

  protected def submitAndWait(
      token: Option[String],
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  )(implicit ec: ExecutionContext): Future[Empty] =
    service(token)
      .submitAndWait(dummySubmitAndWaitRequest(actAs, readAs, userId))
      .map(_ => Empty())

}
