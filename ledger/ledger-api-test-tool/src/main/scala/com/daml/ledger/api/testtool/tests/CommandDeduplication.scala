// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertGrpcError
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.digitalasset.timer.Delayed
import com.google.protobuf.duration.Duration
import io.grpc.Status

import scala.concurrent.duration.DurationInt

final class CommandDeduplication(session: LedgerSession) extends LedgerTestSuite(session) {

  /** A deduplicated submission can either
    * succeed (if the participant knows that the original submission has succeeded),
    * or fail with status ALREADY_EXISTS */
  private[this] def assertDeduplicated(result: Either[Throwable, Unit]): Unit = result match {
    case Left(e) => assertGrpcError(e, Status.Code.ALREADY_EXISTS, "")
    case Right(v) => ()
  }

  test(
    "CDSimpleDeduplication",
    "Deduplicate commands within the TTL window",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val ttlSeconds = 5
      val ttl = Duration.of(ttlSeconds.toLong, 0)
      val a = UUID.randomUUID.toString
      val b = UUID.randomUUID.toString

      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        requestA = request.update(_.commands.ttl := ttl, _.commands.commandId := a)

        // Submit command A (first TTL window)
        _ <- ledger.submit(requestA)
        failure1 <- ledger.submit(requestA).failed

        // Wait until the end of first TTL window
        _ <- Delayed.by(ttlSeconds.seconds)(())

        // Submit command A (second TTL window)
        _ <- ledger.submit(requestA)
        failure2 <- ledger.submit(requestA).failed

        // Submit and wait for command B (to get a unique completion for the end of the test)
        submitAndWaitRequest <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        _ <- ledger.submitAndWait(submitAndWaitRequest.update(_.commands.commandId := b))

        // Inspect created contracts
        activeContracts <- ledger.activeContracts(party)
      } yield {
        assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
        assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

        assert(
          activeContracts.size == 3,
          s"There should be 3 active contracts, but received $activeContracts",
        )
      }
  }

}
