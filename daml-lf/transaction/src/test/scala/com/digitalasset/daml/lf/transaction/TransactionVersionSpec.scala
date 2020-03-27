// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
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

  def fixVersion(v: TransactionVersion) =
    VersionTimeline.maxVersion(TransactionVersions.minOutputVersion, v)

  "assignVersion" should {
    "prefer picking an older version" in {
      assignVersion(assignValueVersions(dummyCreateTransaction)) shouldBe fixVersion(
        TransactionVersion("1"))
    }

    "pick version 2 when confronted with newer data" in {
      val usingOptional = dummyCreateTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.AbsoluteContractId])
      assignVersion(assignValueVersions(usingOptional)) shouldBe fixVersion(TransactionVersion("2"))
    }

    "pick version 7 when confronted with exercise result" in {
      val hasExerciseResult = dummyExerciseWithResultTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.AbsoluteContractId])
      assignVersion(assignValueVersions(hasExerciseResult)) shouldBe fixVersion(
        TransactionVersion("7"))
    }

    "pick version 2 when confronted with exercise result" in {
      val hasExerciseResult = dummyExerciseTransaction map3 (identity, identity, v =>
        ValueOptional(Some(v)): Value[Value.AbsoluteContractId])
      assignVersion(assignValueVersions(hasExerciseResult)) shouldBe fixVersion(
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
  import TransactionSpec.{dummyCreateNode, dummyExerciseNode, StringTransaction}
  private[this] val singleId = "a"
  private val dummyCreateTransaction =
    StringTransaction(HashMap(singleId -> dummyCreateNode), ImmArray(singleId))
  private val dummyExerciseWithResultTransaction =
    StringTransaction(HashMap(singleId -> dummyExerciseNode(ImmArray.empty)), ImmArray(singleId))
  private val dummyExerciseTransaction =
    StringTransaction(
      HashMap(singleId -> dummyExerciseNode(ImmArray.empty, false)),
      ImmArray(singleId),
    )

}
