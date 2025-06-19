// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.multisynchronizer

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.platform.IndexComponentTest
import com.digitalasset.canton.protocol.UnassignId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.flatspec.AnyFlatSpec

class MultiSynchronizerIndexComponentTest extends AnyFlatSpec with IndexComponentTest {
  behavior of "MultiSynchronizer contract lookup"

  it should "successfully look up contract, even if only the assigned event is visible" in {
    val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")
    val party = Ref.Party.assertFromString("party1")
    val builder = TxBuilder()
    val contractId = builder.newCid
    val createNode = builder
      .create(
        id = contractId,
        templateId = Ref.Identifier.assertFromString("P:M:T"),
        argument = Value.ValueUnit,
        signatories = Set(party),
        observers = Set.empty,
      )
    val updateId = Ref.TransactionId.assertFromString("UpdateId")
    val recordTime = Time.Timestamp.now()
    ingestUpdates(
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizer1),
          targetSynchronizer = Target(synchronizer2),
          submitter = Option(party),
          unassignId = UnassignId(TestHash.digest(0)),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNode,
            contractMetadata = Bytes.Empty,
            reassignmentCounter = 15L,
            nodeId = 0,
          )
        ),
        repairCounter = RepairCounter.Genesis,
        recordTime = CantonTimestamp(recordTime),
        synchronizerId = synchronizer2,
      )
    )

    index
      .lookupActiveContract(Set(party), contractId)
      .map { activeContract =>
        activeContract.map(_.createArg) shouldBe Some(
          createNode.versionedCoinst.unversioned.arg
        )
        activeContract.map(_.templateId) shouldBe Some(
          createNode.versionedCoinst.unversioned.template
        )
      }
      .futureValue
  }
}
