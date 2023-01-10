// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.LedgerBridge.toOffset
import com.daml.ledger.sandbox.bridge.validate.SequencerState.SequencerQueue
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.Metrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.chaining._

class SequencerStateSpec extends AnyFlatSpec with Matchers {
  private implicit val bridgeMetrics: BridgeMetrics = new BridgeMetrics(
    Metrics.ForTesting.dropwizardFactory
  )

  "enqueue" should "update the sequencer state" in {
    val Seq(offset1, offset2, offset3) = (1L to 3L).map(toOffset)

    val updatedKeys1 = Map(key(1) -> Some(cid(1)))
    val updatedKeys2 = Map(key(2) -> Some(cid(2)))
    val updatedKeys3 = Map(key(2) -> None)

    val consumedContracts1 = Set(cid(0))
    val consumedContracts2 = Set.empty[ContractId]
    val consumedContracts3 = Set(cid(2))

    val finalState = SequencerState.empty
      .enqueue(offset1, updatedKeys1, consumedContracts1)
      .enqueue(offset2, updatedKeys2, consumedContracts2)
      .enqueue(offset3, updatedKeys3, consumedContracts3)

    finalState.sequencerQueue shouldBe Vector(
      offset1 -> (updatedKeys1, consumedContracts1),
      offset2 -> (updatedKeys2, consumedContracts2),
      offset3 -> (updatedKeys3, consumedContracts3),
    )

    finalState.keyState shouldBe Map(
      key(1) -> (Some(cid(1)), offset1),
      key(2) -> (None, offset3),
    )

    finalState.consumedContractsState shouldBe Set(cid(0), cid(2))
  }

  "enqueue" should "throw on offset not after its last ingested offset" in {
    val populatedSequencerState =
      SequencerState.empty
        .enqueue(toOffset(1L), Map.empty, Set.empty)
        .enqueue(toOffset(3L), Map.empty, Set.empty)

    val act = (idx: Long) => populatedSequencerState.enqueue(toOffset(idx), Map.empty, Set.empty)
    val expectedMessage = (idx: Long) =>
      s"Offset to be enqueued (Offset(Bytes(000000000000000$idx))) is not higher than the last enqueued offset (Offset(Bytes(0000000000000003)))"

    the[RuntimeException] thrownBy act(2L) should have message expectedMessage(2)
    the[RuntimeException] thrownBy act(3L) should have message expectedMessage(3)
  }

  "dequeue" should "evict entries" in {
    val Seq(offsetBefore, offset1, offset2, offset3, offset4, offsetAfter) =
      (0L to 5L).map(toOffset)

    val stateInput: SequencerQueue = Vector(
      offset1 -> (Map(key(1L) -> Some(cid(1))), Set.empty),
      offset2 -> (Map(key(1L) -> None, key(2L) -> None), Set(cid(1))),
      offset3 -> (Map(key(3L) -> Some(cid(3))), Set.empty),
      offset4 -> (Map.empty, Set(cid(4))),
    )
    val initialKeyState = Map(
      key(1L) -> (None, offset2),
      key(2L) -> (None, offset2),
      key(3L) -> (Some(cid(3)), offset3),
    )
    val initialConsumedContractsState = Set(cid(1), cid(4))

    val populatedSequencerState =
      stateInput.foldLeft(SequencerState.empty) {
        case (state, (offset, (updatedKeys, consumedContracts))) =>
          state.enqueue(offset, updatedKeys, consumedContracts)
      }

    populatedSequencerState
      .dequeue(offsetBefore)
      .tap {
        _ shouldBe populatedSequencerState
      }
      .dequeue(offset1)
      .tap { actualState =>
        actualState.sequencerQueue shouldBe stateInput.drop(1)
        actualState.keyState shouldBe initialKeyState
        actualState.consumedContractsState shouldBe initialConsumedContractsState
      }
      .dequeue(offset2)
      .tap { actualState =>
        actualState.sequencerQueue shouldBe stateInput.drop(2)
        actualState.keyState shouldBe Map(key(3L) -> (Some(cid(3)), offset3))
        actualState.consumedContractsState shouldBe Set(cid(4))
      }
      .dequeue(offset4)
      .tap { _ shouldBe SequencerState.empty }
      .dequeue(offsetAfter)
      .tap { _ shouldBe SequencerState.empty }
  }

  private def key(i: Long) = {
    val templateId = Ref.Identifier.assertFromString("pkg:M:T")
    GlobalKey(templateId, Value.ValueInt64(i))
  }

  private def cid(i: Int): ContractId = ContractId.V1(Hash.hashPrivateKey(i.toString))
}
