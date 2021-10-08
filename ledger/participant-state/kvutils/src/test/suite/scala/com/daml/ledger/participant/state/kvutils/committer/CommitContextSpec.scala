// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.committer.CommitContextSpec._
import com.daml.ledger.participant.state.kvutils.store.{DamlPartyAllocation, DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{DamlStateMap, Err, TestHelpers}
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CommitContextSpec extends AnyWordSpec with Matchers {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "get" should {
    "check output first" in {
      val context = newInstance(inputs = newDamlStateMap(aKey -> anotherValue))
      context.set(aKey, aValue)
      context.get(aKey) shouldBe Some(aValue)
    }

    "return input if key has not been output" in {
      val context = newInstance(inputs = newDamlStateMap(aKey -> aValue))
      context.get(aKey) shouldBe Some(aValue)
    }

    "record all accessed input keys" in {
      val context =
        newInstance(inputs = newDamlStateMap(aKey -> aValue, anotherKey -> anotherValue))
      context.get(aKey)
      context.get(anotherKey)

      context.getAccessedInputKeys shouldBe Set(aKey, anotherKey)
    }

    "not record input keys that are not accessed" in {
      val context =
        newInstance(inputs = newDamlStateMap(aKey -> aValue, anotherKey -> anotherValue))
      context.get(aKey)

      context.getAccessedInputKeys shouldBe Set(aKey)
    }

    "throw in case key cannot be found" in {
      val context = newInstance()
      assertThrows[Err.MissingInputState](context.get(aKey))
      context.getAccessedInputKeys shouldBe Set.empty
    }
  }

  "read" should {
    "return input key and record its access" in {
      val context =
        newInstance(inputs = newDamlStateMap(aKey -> aValue, anotherKey -> anotherValue))

      context.read(aKey) shouldBe Some(aValue)
      context.getAccessedInputKeys shouldBe Set(aKey)
    }

    "record key as accessed even if it is not available in the input" in {
      val context = newInstance(inputs = Map(aKey -> None))

      context.read(aKey) shouldBe None
      context.getAccessedInputKeys shouldBe Set(aKey)
    }

    "throw in case key cannot be found" in {
      val context = newInstance()
      assertThrows[Err.MissingInputState](context.read(aKey))
      context.getAccessedInputKeys shouldBe Set.empty
    }
  }

  "collectInputs" should {
    "return keys matching the predicate and mark all inputs as accessed" in {
      val expectedKey1 = aKeyWithContractId("a1")
      val expectedKey2 = aKeyWithContractId("a2")
      val expected = Map(
        expectedKey1 -> Some(aValue),
        expectedKey2 -> None,
      )
      val inputs = expected ++ Map(aKeyWithContractId("b") -> Some(aValue))
      val context = newInstance(inputs = inputs)

      context
        .collectInputs[DamlStateKey, Seq[DamlStateKey]] {
          case (key, _) if key.getContractId.startsWith("a") => key
        }
        .toSet shouldBe expected.keys
      context.getAccessedInputKeys shouldBe inputs.keys
    }

    "return no keys and mark all inputs as accessed for a predicate producing empty output" in {
      val context =
        newInstance(inputs = newDamlStateMap(aKey -> aValue, anotherKey -> anotherValue))

      context.collectInputs[Unit, Seq[Unit]] { case _ if false => () } shouldBe empty
      context.getAccessedInputKeys shouldBe Set(aKey, anotherKey)
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
      val context = newInstance(inputs = newDamlStateMap(aKey -> aValue))
      context.set(aKey, aValue)
      context.getOutputs should have size 0
    }

    "output a key whose value has changed from its input value" in {
      val context = newInstance(inputs = newDamlStateMap(aKey -> aValue))
      context.set(aKey, anotherValue)
      context.getOutputs.toSeq shouldBe Seq((aKey, anotherValue))
    }

    "output last set value for a key that was also input" in {
      val context = newInstance(inputs = newDamlStateMap(aKey -> aValue))

      context.set(aKey, anotherValue)
      context.set(aKey, aValue)

      context.getOutputs should have size 0
    }
  }

  "preExecute" should {
    "return false in case record time is set" in {
      val context = newInstance(recordTime = Some(Time.Timestamp.now()))
      context.preExecute shouldBe false
    }

    "return true in case record time is not set" in {
      val context = newInstance(recordTime = None)
      context.preExecute shouldBe true
    }
  }
}

object CommitContextSpec {
  private def aKeyWithContractId(id: String): DamlStateKey =
    DamlStateKey.newBuilder.setContractId(id).build

  private val aKey: DamlStateKey = aKeyWithContractId("contract ID 1")
  private val anotherKey: DamlStateKey = aKeyWithContractId("contract ID 2")
  private val aValue: DamlStateValue = DamlStateValue.newBuilder
    .setParty(DamlPartyAllocation.newBuilder.setDisplayName("a party name"))
    .build
  private val anotherValue: DamlStateValue = DamlStateValue.newBuilder
    .setParty(DamlPartyAllocation.newBuilder.setDisplayName("another party name"))
    .build

  private def newInstance(
      recordTime: Option[Time.Timestamp] = Some(Time.Timestamp.now()),
      inputs: DamlStateMap = Map.empty,
  ) =
    CommitContext(inputs, recordTime, TestHelpers.mkParticipantId(1))

  private def newDamlStateMap(keyAndValues: (DamlStateKey, DamlStateValue)*): DamlStateMap =
    (for ((key, value) <- keyAndValues)
      yield (key, Some(value))).toMap
}
