// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Err.MissingInputState
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, DamlStateMap, TestHelpers}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time
import org.scalatest.{Matchers, WordSpec}

class CommitContextSpec extends WordSpec with Matchers {
  "get" should {
    "check output first" in {
      val context = newInstance(Map(aKey -> Some(anotherValue)))
      context.set(aKey, aValue)
      context.get(aKey) shouldBe Some(aValue)
    }

    "return input if key has not been output" in {
      val context = newInstance(Map(aKey -> Some(aValue)))
      context.get(aKey) shouldBe Some(aValue)
    }

    // If the access order is deterministic, then the iteration order is not just stable but also deterministic.
    "records all accessed input keys and iterates over them in a stable order" in {
      val context = newInstance(Map(aKey -> Some(aValue), anotherKey -> Some(anotherValue)))
      context.get(aKey)
      context.get(anotherKey)
      context.get(aKey)

      context.getAccessedInputKeys.toSeq shouldBe Seq(aKey, anotherKey)
    }

    "throw in case key cannot be found" in {
      val context = newInstance()
      assertThrows[MissingInputState](context.get(aKey))
    }
  }

  "set" should {
    "maintain order of keys based on when they were seen first" in {
      val context = newInstance()

      context.set(aKey, aValue)
      context.set(anotherKey, anotherValue)
      context.set(aKey, anotherValue)

      context.getOutputs.map(_._1).toSeq shouldBe Seq(aKey, anotherKey)
    }

    "update value for an already output key" in {
      val context = newInstance()

      context.set(aKey, aValue)
      context.set(aKey, anotherValue)

      context.getOutputs.toSeq shouldBe Seq((aKey, anotherValue))
    }

    "not output a key whose value is identical to its input value" in {
      val context = newInstance(Map(aKey -> Some(aValue)))
      context.set(aKey, aValue)
      context.getOutputs should have size 0
    }

    "output a key whose value has changed from its input value" in {
      val context = newInstance(Map(aKey -> Some(aValue)))
      context.set(aKey, anotherValue)
      context.getOutputs.toSeq shouldBe Seq((aKey, anotherValue))
    }

    "output last set value for a key that was also input" in {
      val context = newInstance(Map(aKey -> Some(aValue)))

      context.set(aKey, anotherValue)
      context.set(aKey, aValue)

      context.getOutputs should have size 0
    }
  }

  private val aKey: DamlStateKey = DamlStateKey.newBuilder.setContractId("contract ID 1").build
  private val anotherKey: DamlStateKey =
    DamlStateKey.newBuilder.setContractId("contract ID 2").build
  private val aValue: DamlStateValue = DamlStateValue.newBuilder
    .setParty(DamlPartyAllocation.newBuilder.setDisplayName("a party name"))
    .build
  private val anotherValue: DamlStateValue = DamlStateValue.newBuilder
    .setParty(DamlPartyAllocation.newBuilder.setDisplayName("another party name"))
    .build

  private class TestCommitContext(override val inputs: DamlStateMap) extends CommitContext {
    override def getEntryId: DamlKvutils.DamlLogEntryId = DamlLogEntryId.getDefaultInstance

    override def getMaximumRecordTime: Time.Timestamp = Time.Timestamp.now()

    override def getRecordTime: Time.Timestamp = Time.Timestamp.now()

    override def getParticipantId: ParticipantId = TestHelpers.mkParticipantId(1)
  }

  private def newInstance(inputs: DamlStateMap = Map.empty) = new TestCommitContext(inputs)
}
