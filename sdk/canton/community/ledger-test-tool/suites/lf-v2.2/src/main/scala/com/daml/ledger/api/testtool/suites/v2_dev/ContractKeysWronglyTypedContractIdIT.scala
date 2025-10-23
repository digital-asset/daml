// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.experimental.test.{Delegated, Delegation}
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

final class ContractKeysWronglyTypedContractIdIT extends LedgerTestSuite {
  import ContractKeysCompanionImplicits.*

  test("WTFetchFails", "Fetching of the wrong type fails", allocate(SingleParty))(implicit ec => {
    case Participants(Participant(ledger, Seq(party))) =>
      for {
        dummy <- ledger.create(party, new Dummy(party))
        fakeDelegated = new Delegated.ContractId(dummy.contractId)
        delegation: Delegation.ContractId <- ledger.create(party, new Delegation(party, party))

        fetchFailure <- ledger
          .exercise(party, delegation.exerciseFetchDelegated(fakeDelegated))
          .mustFail("fetching the wrong type")
      } yield {
        assertGrpcError(
          fetchFailure,
          CommandExecutionErrors.Interpreter.WronglyTypedContract,
          Some("wrongly typed contract id"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })
}
