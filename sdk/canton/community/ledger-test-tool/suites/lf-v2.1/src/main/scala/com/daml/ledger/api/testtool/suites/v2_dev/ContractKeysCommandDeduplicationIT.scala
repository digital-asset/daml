// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.test.java.experimental.da.types.Tuple2
import com.daml.ledger.test.java.experimental.test.{TextKey, TextKeyOperations}
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.daml.lf.data.Ref.SubmissionId

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

final class ContractKeysCommandDeduplicationIT extends LedgerTestSuite {

  import ContractKeysCompanionImplicits.*

  test(
    s"StopOnCompletionFailure",
    "Stop deduplicating commands on completion failure",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val key = ledger.nextKeyId()

    for {
      // Create a helper and a text key
      ko <- ledger.create(party, new TextKeyOperations(party))
      _ <- ledger.create(party, new TextKey(party, key, List().asJava))

      // Create two competing requests
      requestA = ledger.submitAndWaitRequest(
        party,
        ko.exerciseTKOFetchAndRecreate(new Tuple2(party.getValue, key)).commands,
      )
      requestB = ledger.submitAndWaitRequest(
        party,
        ko.exerciseTKOFetchAndRecreate(new Tuple2(party.getValue, key)).commands,
      )

      // Submit both requests in parallel.
      // Either both succeed (if one transaction is recorded faster than the other submission starts command interpretation, unlikely)
      // Or one submission is rejected (if one transaction is recorded during the call of lookupMaximumLedgerTime() in [[LedgerTimeHelper]], unlikely)
      // Or one transaction is rejected (this is what we want to test)
      submissionResults <- Future.traverse(List(requestA, requestB))(request =>
        ledger.submitAndWait(request).transform(result => Success(request -> result))
      )

      // Resubmit a failed command.
      // No matter what the rejection reason was (hopefully it was a rejected transaction),
      // a resubmission of exactly the same command should succeed.
      _ <- submissionResults
        .collectFirst { case (request, Failure(_)) => request }
        .fold(Future.unit)(request =>
          ledger.submitAndWait(updateWithFreshSubmissionId(request)).map(_ => ())
        )
    } yield {
      ()
    }
  })

  private def updateWithFreshSubmissionId(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    request.update(_.commands.submissionId := newSubmissionId())

  private def newSubmissionId(): SubmissionId = SubmissionIdGenerator.Random.generate()

  val defaultCantonSkew = 365.days

}
