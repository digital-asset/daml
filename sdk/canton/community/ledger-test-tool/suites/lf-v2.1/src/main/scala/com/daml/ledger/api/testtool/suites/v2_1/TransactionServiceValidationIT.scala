// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.update_service.{GetUpdateByIdRequest, GetUpdateByOffsetRequest}
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.protocol.TestUpdateId

class TransactionServiceValidationIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "TXRejectEmptyFilter",
    "A query with an empty transaction filter should be rejected with an INVALID_ARGUMENT status",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      request <- ledger.getTransactionsRequest(ledger.transactionFormat(Some(Seq(party))))
      requestWithEmptyFilter = request.update(
        _.updateFormat.includeTransactions.eventFormat.filtersByParty := Map.empty
      )
      failure <- ledger
        .transactions(requestWithEmptyFilter)
        .mustFail("subscribing with empty filtersByParty and filtersForAnyParty")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.InvalidArgument,
        Some("filtersByParty and filtersForAnyParty cannot be empty simultaneously"),
      )
    }
  })

  test(
    "TXRejectBeginAfterEnd",
    "A request with the end before the begin should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      earlier <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      later <- ledger.currentEnd()
      request <- ledger.getTransactionsRequest(ledger.transactionFormat(Some(Seq(party))))
      invalidRequest = request.update(
        _.beginExclusive := later,
        _.endInclusive := earlier,
      )
      failure <- ledger
        .transactions(invalidRequest)
        .mustFail("subscribing with the end before the begin")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.OffsetOutOfRange,
        Some("is before begin offset"),
      )
    }
  })

  test(
    "TXUpdateByIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up an update by identifier without specifying an update format",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      failure <- ledger
        .updateById(
          GetUpdateByIdRequest(
            TestUpdateId("not-relevant").toHexString,
            None,
          )
        )
        .mustFail("looking up an update by id without an update format")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.MissingField,
        Some(
          "The submitted command is missing a mandatory field: update_format"
        ),
      )
    }
  })

  test(
    "TXTransactionByOffsetInvalid",
    "Return INVALID_ARGUMENT when looking up a flat transaction using an invalid offset",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      failure <- ledger
        .transactionByOffset(-21, Seq(party), AcsDelta)
        .mustFail("looking up a flat transaction using an invalid offset")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NonPositiveOffset,
        Some("Offset -21 in offset is not a positive integer"),
      )
    }
  })

  test(
    "TXUpdateByOffsetWithoutParty",
    "Return INVALID_ARGUMENT when looking up an update by offset without specifying a party or update format",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      failure <- ledger
        .updateByOffset(GetUpdateByOffsetRequest(offset = 42, updateFormat = None))
        .mustFail("looking up an update without specifying a format")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.MissingField,
        Some(
          "The submitted command is missing a mandatory field: update_format"
        ),
      )
    }
  })
}
