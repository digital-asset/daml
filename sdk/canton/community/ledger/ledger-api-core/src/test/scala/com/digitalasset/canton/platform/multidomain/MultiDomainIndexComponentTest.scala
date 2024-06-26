// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.multidomain

import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.value.Value
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.platform.IndexComponentTest
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.Traced
import org.scalatest.flatspec.AsyncFlatSpec

class MultiDomainIndexComponentTest extends AsyncFlatSpec with IndexComponentTest {
  behavior of "MultiDomain contract lookup"

  it should "successfully look up contract, even if only the assigned event is visible" in {
    val domain1 = DomainId.tryFromString("x::domain1")
    val domain2 = DomainId.tryFromString("x::domain2")
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

    ingestUpdates(
      Traced(
        Update.ReassignmentAccepted(
          optCompletionInfo = None,
          workflowId = None,
          updateId = updateId,
          recordTime = Time.Timestamp.now(),
          reassignmentInfo = ReassignmentInfo(
            sourceDomain = SourceDomainId(domain1),
            targetDomain = TargetDomainId(domain2),
            submitter = Option(party),
            reassignmentCounter = 15L,
            hostedStakeholders = List(party),
            unassignId = CantonTimestamp.now(),
          ),
          reassignment = Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNode,
            contractMetadata = Bytes.Empty,
          ),
        )
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
  }
}
