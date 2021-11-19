// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com
package daml.ledger.api.testtool.suites

import daml.error.definitions.LedgerApiErrors
import daml.ledger.api.testtool.infrastructure.Allocation._
import daml.ledger.api.testtool.infrastructure.Assertions._
import daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import daml.ledger.test.semantic.Exceptions._

import io.grpc.Status

// This test suite contains tests with assertions adapted specifically Sandbox classic with in-memory ledger backend (deprecated).
// It asserts a more generic `INCONSISTENT` error code instead of `INCONSISTENT_CONTRACT_KEY` or `DUPLICATE_CONTRACT_KEY`
// TODO sandbox-classic removal: Remove this tests
final class DeprecatedSandboxClassicMemoryExceptionsIT extends LedgerTestSuite {

  test(
    "ExRollbackDuplicateKeyCreated",
    "Rollback fails once contract with same key is created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseDuplicateKey(_))
      _ <- ledger.create(party, WithSimpleKey(party))
      failure <- ledger.exercise(party, t.exerciseDuplicateKey(_)).mustFail("duplicate key")
    } yield {
      assertGrpcError(
        ledger,
        failure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.Inconsistent,
        Some("DuplicateKey"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRollbackDuplicateKeyArchived",
    "Rollback succeeds once contract with same key is archived",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, ExceptionTester(party))
      withKey <- ledger.create(party, WithSimpleKey(party))
      failure <- ledger.exercise(party, t.exerciseDuplicateKey(_)).mustFail("duplicate key")
      _ = assertGrpcError(
        ledger,
        failure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.Inconsistent,
        Some("DuplicateKey"),
        checkDefiniteAnswerMetadata = true,
      )
      _ <- ledger.exercise(party, withKey.exerciseArchive(_))
      _ <- ledger.exercise(party, t.exerciseDuplicateKey(_))
    } yield ()
  })
}
