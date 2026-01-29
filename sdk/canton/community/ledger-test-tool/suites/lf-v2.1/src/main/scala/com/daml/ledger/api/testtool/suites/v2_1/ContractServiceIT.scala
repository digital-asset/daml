// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

class ContractServiceIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "CSNotFound",
    "The ContractService should fail with a CONTRACT_PAYLOAD_NOT_FOUND response if contracts has no intersection with the querying parties",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, otherParty))) =>
    for {
      contract <- ledger.create(party, new Dummy(party))
      error <- ledger.contract(Some(Seq(otherParty)), contract.contractId).failed
    } yield {
      assertGrpcError(
        error,
        RequestValidationErrors.NotFound.ContractPayload,
        Some("Contract payload not found, or not visible."),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSanyParty",
    "The ContractService should succeed with a contract for no querying parties",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      contract <- ledger.create(party, new Dummy(party))
      result <- ledger.contract(None, contract.contractId)
    } yield {
      assert(
        result.nonEmpty,
        s"There should be a contract found, but received none",
      )

      assertEquals(
        context =
          s"The contract ID should be the same as the created contract, expected: ${contract.contractId} and found: ${result.map(_.contractId).getOrElse("")}",
        actual = result.map(_.contractId).getOrElse(""),
        expected = contract.contractId,
      )

      assert(
        result.value.createdEventBlob.size() > 0,
        s"The createdEventBlob should be populated but it is empty",
      )
    }
  })

  test(
    "CSforParty",
    "The ContractService should succeed with a contract for stakeholder parties",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      contract <- ledger.create(party, new Dummy(party))
      result <- ledger.contract(Some(Seq(party)), contract.contractId)
    } yield {
      assert(
        result.nonEmpty,
        s"There should be a contract found, but received none",
      )

      assertEquals(
        context =
          s"The contract ID should be the same as the created contract, expected: ${contract.contractId} and found: ${result.map(_.contractId).getOrElse("")}",
        actual = result.map(_.contractId).getOrElse(""),
        expected = contract.contractId,
      )

      assert(
        result.value.createdEventBlob.size() > 0,
        s"The createdEventBlob should be populated but it is empty",
      )
    }
  })
}
