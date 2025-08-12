// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.multisynchronizer

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.platform.IndexComponentTest
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class MultiSynchronizerIndexComponentTest extends AnyFlatSpec with IndexComponentTest {
  behavior of "MultiSynchronizer contract lookup"

  private val sequentiallyPostProcessedUpdates = mutable.Buffer[Update]()

  override protected def sequentialPostProcessor: Update => Unit =
    sequentiallyPostProcessedUpdates.append

  it should "successfully look up contract, even if only the assigned event is visible" in {
    val party = Ref.Party.assertFromString("party1")

    val (reassignmentAccepted1, cn1) =
      mkReassignmentAccepted(party, "UpdateId1", withAcsChange = false)
    val (reassignmentAccepted2, cn2) =
      mkReassignmentAccepted(party, "UpdateId2", withAcsChange = true)
    ingestUpdates(reassignmentAccepted1, reassignmentAccepted2)

    (for {
      activeContractO1 <- index.lookupActiveContract(Set(party), cn1.coid)
      activeContractO2 <- index.lookupActiveContract(Set(party), cn2.coid)
    } yield {
      Seq(cn1 -> activeContractO1, cn2 -> activeContractO2).foreach { case (cn, activeContractO) =>
        activeContractO.map(_.createArg) shouldBe Some(cn.versionedCoinst.unversioned.arg)
        activeContractO.map(_.templateId) shouldBe Some(cn.versionedCoinst.unversioned.template)
      }
    }).futureValue

    // Verify that the AcsChanges have been propagated to the sequential post-processor.
    sequentiallyPostProcessedUpdates.count {
      case _: Update.OnPRReassignmentAccepted => true
      case _ => false
    } shouldBe 1
    sequentiallyPostProcessedUpdates.count {
      case _: Update.RepairReassignmentAccepted => true
      case _ => false
    } shouldBe 1
  }

  private def mkReassignmentAccepted(
      party: Ref.Party,
      updateIdS: String,
      withAcsChange: Boolean,
  ): (Update.ReassignmentAccepted, Node.Create) = {
    val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")
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
    val updateId = Ref.TransactionId.assertFromString(updateIdS)
    val recordTime = Time.Timestamp.now()
    (
      if (withAcsChange)
        Update.OnPRReassignmentAccepted(
          workflowId = None,
          updateId = updateId,
          reassignmentInfo = ReassignmentInfo(
            sourceSynchronizer = Source(synchronizer1),
            targetSynchronizer = Target(synchronizer2),
            submitter = Option(party),
            reassignmentId = ReassignmentId.tryCreate("00"),
            isReassigningParticipant = true,
          ),
          reassignment = Reassignment.Batch(
            Reassignment.Assign(
              ledgerEffectiveTime = Time.Timestamp.now(),
              createNode = createNode,
              contractAuthenticationData = Bytes.Empty,
              reassignmentCounter = 15L,
              nodeId = 0,
            )
          ),
          repairCounter = RepairCounter.Genesis,
          recordTime = CantonTimestamp(recordTime),
          synchronizerId = synchronizer2,
          acsChangeFactory = TestAcsChangeFactory(),
        )
      else
        Update.RepairReassignmentAccepted(
          workflowId = None,
          updateId = updateId,
          reassignmentInfo = ReassignmentInfo(
            sourceSynchronizer = Source(synchronizer1),
            targetSynchronizer = Target(synchronizer2),
            submitter = Option(party),
            reassignmentId = ReassignmentId.tryCreate("00"),
            isReassigningParticipant = true,
          ),
          reassignment = Reassignment.Batch(
            Reassignment.Assign(
              ledgerEffectiveTime = Time.Timestamp.now(),
              createNode = createNode,
              contractAuthenticationData = Bytes.Empty,
              reassignmentCounter = 15L,
              nodeId = 0,
            )
          ),
          repairCounter = RepairCounter.Genesis,
          recordTime = CantonTimestamp(recordTime),
          synchronizerId = synchronizer2,
        ),
      createNode,
    )
  }
}
