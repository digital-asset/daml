// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.multidomain

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.platform.IndexComponentTest
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.flatspec.AnyFlatSpec

class MultiDomainIndexComponentTest extends AnyFlatSpec with IndexComponentTest {
  behavior of "MultiDomain contract lookup"

  it should "successfully look up contract, even if only the assigned event is visible" in {
    val domain1 = SynchronizerId.tryFromString("x::domain1")
    val domain2 = SynchronizerId.tryFromString("x::domain2")
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
          sourceDomain = Source(domain1),
          targetDomain = Target(domain2),
          submitter = Option(party),
          reassignmentCounter = 15L,
          hostedStakeholders = List(party),
          unassignId = CantonTimestamp.now(),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Assign(
          ledgerEffectiveTime = Time.Timestamp.now(),
          createNode = createNode,
          contractMetadata = Bytes.Empty,
        ),
        requestCounter = RequestCounter(0),
        recordTime = CantonTimestamp(recordTime),
      )
    )

    index
      .lookupActiveContract(Set(party), contractId)
      .map { activeContract =>
        activeContract.map(_.unversioned.arg) shouldBe Some(
          createNode.versionedCoinst.unversioned.arg
        )
        activeContract.map(_.unversioned.template) shouldBe Some(
          createNode.versionedCoinst.unversioned.template
        )
      }
      .futureValue
  }
}
