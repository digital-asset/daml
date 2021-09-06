// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.CompletionDeduplicationInfoIT._
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy

import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT(service: Service) extends LedgerTestSuite {

  override private[testtool] def name = service.productPrefix + super.name

  test(
    "CCDIIncludeApplicationId",
    "The application ID is present in completions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- submitSuccessfulAndFailingRequests(service, ledger, party)
    } yield {
    }
  })
}

private[testtool] object CompletionDeduplicationInfoIT {
  sealed trait Service extends Serializable with Product
  case object CommandService extends Service
  case object CommandSubmissionService extends Service

  private def submitSuccessfulAndFailingRequests(
      service: Service,
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      updateSuccessfulCommandServiceRequest: SubmitAndWaitRequest => SubmitAndWaitRequest =
        identity,
      updateSuccessfulCommandSubmissionServiceRequest: SubmitRequest => SubmitRequest = identity,
  )(implicit ec: ExecutionContext): Future[Unit] =
    service match {
      case CommandService =>
        val successfulRequest = ledger.submitAndWaitRequest(party, successfulCommand(party))
        for {
          _ <- ledger.submitAndWait(updateSuccessfulCommandServiceRequest(successfulRequest))
        } yield ()
      case CommandSubmissionService =>
        val successfulRequest = ledger.submitRequest(party, successfulCommand(party))
        for {
          _ <- ledger.submit(updateSuccessfulCommandSubmissionServiceRequest(successfulRequest))
        } yield ()
    }

  private def successfulCommand(party: Primitive.Party) = Dummy(party).create.command
}
