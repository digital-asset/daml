// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, ReassignmentId}
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReassignmentCommandsBatchTest extends AnyWordSpec with Matchers {
  private val cid1 = ExampleTransactionFactory.suffixedId(-1, 0)
  private val cid2 = ExampleTransactionFactory.suffixedId(-1, 1)
  private val cid3 = ExampleTransactionFactory.suffixedId(-1, 2)

  private def synchronizerId(i: Int) = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive(s"synchronizer::source_$i")
  )

  private val unassign =
    ReassignmentCommand.Unassign(Source(synchronizerId(1)), Target(synchronizerId(2)), cid1)
  private val assign = ReassignmentCommand.Assign(
    Source(synchronizerId(1)),
    Target(synchronizerId(2)),
    CantonTimestamp.Epoch,
  )

  "ReassignmentCommandsBatch.create" when {
    "with no commands should fail" in {
      ReassignmentCommandsBatch.create(Nil) shouldBe Left(ReassignmentCommandsBatch.NoCommands)
    }

    "with one unassign should succeed" in {
      ReassignmentCommandsBatch.create(Seq(unassign)) shouldBe Right(
        ReassignmentCommandsBatch.Unassignments(
          source = unassign.sourceSynchronizer,
          target = unassign.targetSynchronizer,
          contractIds = NonEmpty.mk(Seq, unassign.contractId),
        )
      )
    }

    "with one assign should succeed" in {
      ReassignmentCommandsBatch.create(Seq(assign)) shouldBe Right(
        ReassignmentCommandsBatch.Assignments(
          target = assign.targetSynchronizer,
          reassignmentId = ReassignmentId(assign.sourceSynchronizer, assign.unassignId),
        )
      )
    }

    "with multiple unassigns with same source and target should succeed" in {
      ReassignmentCommandsBatch.create(
        Seq(
          unassign,
          unassign.copy(contractId = cid2),
          unassign.copy(contractId = cid3),
        )
      ) shouldBe Right(
        ReassignmentCommandsBatch.Unassignments(
          source = unassign.sourceSynchronizer,
          target = unassign.targetSynchronizer,
          contractIds = NonEmpty.apply(Seq, cid1, cid2, cid3),
        )
      )
    }

    "with multiple unassign with different source should fail" in {
      ReassignmentCommandsBatch.create(
        Seq(
          unassign,
          unassign.copy(sourceSynchronizer = Source(synchronizerId(42))),
        )
      ) shouldBe Left(
        ReassignmentCommandsBatch.UnassignmentsWithDifferingSynchronizers
      )
    }

    "with multiple unassign with different target should fail" in {
      ReassignmentCommandsBatch.create(
        Seq(
          unassign,
          unassign.copy(targetSynchronizer = Target(synchronizerId(42))),
        )
      ) shouldBe Left(ReassignmentCommandsBatch.UnassignmentsWithDifferingSynchronizers)
    }

    "with multiple assigns should fail" in {
      ReassignmentCommandsBatch.create(
        Seq(
          assign,
          assign.copy(unassignId = assign.unassignId.plusSeconds(1)),
        )
      ) shouldBe Left(ReassignmentCommandsBatch.MixedAssignmentWithOtherCommands)
    }

    "with both assigns and unassign should fail" in {
      ReassignmentCommandsBatch.create(
        Seq[ReassignmentCommand](
          unassign,
          assign,
        )
      ) shouldBe Left(ReassignmentCommandsBatch.MixedAssignmentWithOtherCommands)
    }
  }
}
