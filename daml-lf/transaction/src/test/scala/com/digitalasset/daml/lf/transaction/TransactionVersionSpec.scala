// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import data.ImmArray
import value.Value
import Value.{ContractId, ValueOptional, VersionedValue}
import value.ValueVersions.asVersionedValue
import TransactionVersions.assignVersion
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.HashMap

class TransactionVersionSpec extends WordSpec with Matchers {
  import TransactionVersionSpec._

  import VersionTimeline.maxVersion

  // FIXME: https://github.com/digital-asset/daml/issues/5164
  // Currently the engine uses `TransactionVersions.minOutputVersion` or latter.
  // #5164 should provide a more granular way to control the versioning.
  // Once #5164 is resolved, update those tests with different values for `minOutputVersion`.
  val minOutputVersion = TransactionVersions.minOutputVersion

  "assignVersion" should {
    "prefer picking an older version" in {
      assignVersion(assignValueVersions(dummyCreateTransaction)) shouldBe maxVersion(
        minOutputVersion,
        TransactionVersion("1"))
    }

    "pick version 2 when confronted with newer data" in {
      val usingOptional = dummyCreateTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.ContractId])
      assignVersion(assignValueVersions(usingOptional)) shouldBe maxVersion(
        minOutputVersion,
        TransactionVersion("2"))
    }

    "pick version 7 when confronted with exercise result" in {
      val hasExerciseResult = dummyExerciseWithResultTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.ContractId])
      assignVersion(assignValueVersions(hasExerciseResult)) shouldBe maxVersion(
        minOutputVersion,
        TransactionVersion("7"))
    }

    "pick version 2 when confronted with exercise result" in {
      val hasExerciseResult = dummyExerciseTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.ContractId])
      assignVersion(assignValueVersions(hasExerciseResult)) shouldBe maxVersion(
        minOutputVersion,
        TransactionVersion("2"))
    }

  }

  private[this] def assignValueVersions[Nid, Cid <: ContractId](
      t: GenTransaction[Nid, Cid, Value[Cid]],
  ): GenTransaction[Nid, Cid, VersionedValue[Cid]] =
    t map3 (identity, identity, v =>
      asVersionedValue(v) fold (e =>
        fail(s"We didn't write traverse for GenTransaction: $e"), identity))
}

object TransactionVersionSpec {

  import TransactionSpec._

  private[this] val singleId = Value.NodeId(0)
  private val dummyCreateTransaction =
    mkTransaction(HashMap(singleId -> dummyCreateNode("cid1")), ImmArray(singleId))
  private val dummyExerciseWithResultTransaction =
    mkTransaction(
      HashMap(singleId -> dummyExerciseNode("cid2", ImmArray.empty)),
      ImmArray(singleId))
  private val dummyExerciseTransaction =
    mkTransaction(
      HashMap(singleId -> dummyExerciseNode("cid3", ImmArray.empty, false)),
      ImmArray(singleId),
    )

}
