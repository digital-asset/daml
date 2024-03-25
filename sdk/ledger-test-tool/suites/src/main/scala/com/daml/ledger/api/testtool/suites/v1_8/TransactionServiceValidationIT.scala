// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test._

import scala.collection.immutable.Seq

class TransactionServiceValidationIT extends LedgerTestSuite {
  import CompanionImplicits._

  test(
    "TXRejectEmptyFilter",
    "A query with an empty transaction filter should be rejected with an INVALID_ARGUMENT status",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
    val requestWithEmptyFilter = request.update(_.filter.filtersByParty := Map.empty)
    for {
      failure <- ledger
        .flatTransactions(requestWithEmptyFilter)
        .mustFail("subscribing with an empty filter")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some("filtersByParty cannot be empty"),
      )
    }
  })

  test(
    "TXRejectBeginAfterEnd",
    "A request with the end before the begin should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      earlier <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      later <- ledger.currentEnd()
      request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      invalidRequest = request.update(_.begin := later, _.end := earlier)
      failure <- ledger
        .flatTransactions(invalidRequest)
        .mustFail("subscribing with the end before the begin")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.OffsetOutOfRange,
        Some("is before Begin offset"),
      )
    }
  })

  test(
    "TXFlatTransactionsWrongLedgerId",
    "The getTransactions endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactions(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXTransactionTreesWrongLedgerId",
    "The getTransactionTrees endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTrees(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXTransactionTreeByIdWrongLedgerId",
    "The getTransactionTreeById endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTreeById(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXFlatTransactionByIdWrongLedgerId",
    "The getFlatTransactionById endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactionById(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXTransactionTreeByEventIdWrongLedgerId",
    "The getTransactionTreeByEventId endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByEventIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTreeByEventId(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXFlatTransactionByEventIdWrongLedgerId",
    "The getFlatTransactionByEventId endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByEventIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactionByEventId(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXLedgerEndWrongLedgerId",
    "The ledgerEnd endpoint should reject calls with the wrong ledger identifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    for {
      failure <- ledger
        .currentEnd(invalidLedgerId)
        .mustFail("requesting with the wrong ledger ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some(s"Ledger ID '$invalidLedgerId' not found."),
      )
    }
  })

  test(
    "TXTransactionTreeByIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a transaction tree by identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .transactionTreeById("not-relevant")
        .mustFail("looking up a transaction tree without specifying a party")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.MissingField,
        Some("requesting_parties"),
      )
    }
  })

  test(
    "TXFlatTransactionByIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a flat transaction by identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .flatTransactionById("not-relevant")
        .mustFail("looking up a flat transaction without specifying a party")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.MissingField,
        Some("requesting_parties"),
      )
    }
  })

  test(
    "TXTransactionTreeByEventIdInvalid",
    "Return INVALID_ARGUMENT when looking up a transaction tree using an invalid event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId("dont' worry, be happy", party)
        .mustFail("looking up an transaction tree using an invalid event ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidField,
        Some("Invalid field event_id"),
      )
    }
  })

  test(
    "TXTransactionTreeByEventIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a transaction tree by event identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId("not-relevant")
        .mustFail("looking up a transaction tree without specifying a party")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.MissingField,
        Some("requesting_parties"),
      )
    }
  })

  test(
    "TXFlatTransactionByEventIdInvalid",
    "Return INVALID_ARGUMENT when looking up a flat transaction using an invalid event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId("dont' worry, be happy", party)
        .mustFail("looking up a flat transaction using an invalid event ID")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidField,
        Some("Invalid field event_id"),
      )
    }
  })

  test(
    "TXFlatTransactionByEventIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a flat transaction by event identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId("not-relevant")
        .mustFail("looking up a flat transaction without specifying a party")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.MissingField,
        Some("requesting_parties"),
      )
    }
  })
}
