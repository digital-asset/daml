// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertGrpcError
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.test_stable.Test.Dummy
import com.daml.ledger.test_stable.Test.Dummy._
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class ParticipantPruning(session: LedgerSession) extends LedgerTestSuite(session) {
  private val logger = LoggerFactory.getLogger(name)

  test(
    "PRFailEmptyPruneByTime",
    "Pruning an empty ledger by time should fail",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        failure <- ledger.pruneByTime(Instant.now.minus(0, ChronoUnit.DAYS)).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          "is too recent. You can prune at most at")
      }
  }

  test(
    "PRPruneByOffsetSucceeds",
    "Pruning a non-empty ledger by offset should succeed and prevent access of pruned state",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val transactionsToSubmit = 14
      for {
        _dummies <- Future.sequence(
          Vector.fill(transactionsToSubmit) {
            for {
              dummy <- ledger.create(party, Dummy(party))
              _ <- ledger.exercise(party, dummy.exerciseDummyChoice1)
            } yield dummy
          }
        )
        transactionTrees <- ledger.transactionTrees(party)
        _ = { assert(transactionTrees.size == transactionsToSubmit * 2) }
        ledgerEnd <- ledger.currentEnd()
        pruneResponse <- ledger.pruneByOffset(ledgerEnd)
        dontReadPrunedTransactionTrees <- ledger.transactionTrees(party).failed
        dontReadPrunedTransactions <- ledger.flatTransactions(party).failed
        dontReadPrunedCompletions <- ledger.firstCompletions(party).failed
      } yield {
        assert(pruneResponse.prunedOffset.contains(ledgerEnd))
        assertGrpcError(
          dontReadPrunedTransactionTrees,
          Status.Code.OUT_OF_RANGE,
          "Out of range offset access violation before or at")
        assertGrpcError(
          dontReadPrunedTransactions,
          Status.Code.OUT_OF_RANGE,
          "Out of range offset access violation before or at")
        assertGrpcError(
          dontReadPrunedCompletions,
          Status.Code.OUT_OF_RANGE,
          "Out of range offset access violation before or at")
      }
  }

}
