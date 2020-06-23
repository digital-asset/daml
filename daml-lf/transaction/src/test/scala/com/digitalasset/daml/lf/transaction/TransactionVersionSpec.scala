// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import data.ImmArray
import value.{Value, ValueVersion, ValueVersions}
import Value.{ContractId, ValueOptional, VersionedValue}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.HashMap

class TransactionVersionSpec extends WordSpec with Matchers {
  import TransactionVersionSpec._

  import VersionTimeline.maxVersion

  private[this] val supportedValVersions =
    ValueVersions.SupportedDevVersions.copy(min = ValueVersion("1"))
  private[this] val supportedTxVersions =
    TransactionVersions.SupportedDevVersions.copy(min = TransactionVersion("1"))

  "assignVersion" should {
    "prefer picking an older version" in {
      assignTxVersion(assignValueVersions(dummyCreateTransaction)) shouldBe Right(
        maxVersion(supportedTxVersions.min, TransactionVersion("1")))
    }

    "pick version 2 when confronted with newer data" in {
      val usingOptional =
        dummyCreateTransaction map3 (identity, identity, v => ValueOptional(Some(v)))
      assignTxVersion(assignValueVersions(usingOptional)) shouldBe Right(
        maxVersion(supportedTxVersions.min, TransactionVersion("2")))
    }

    "pick version 7 when confronted with exercise result" in {
      val hasExerciseResult =
        dummyExerciseWithResultTransaction map3 (identity, identity, v => ValueOptional(Some(v)))
      assignTxVersion(assignValueVersions(hasExerciseResult)) shouldBe Right(
        maxVersion(supportedTxVersions.min, TransactionVersion("7")))
    }

    "pick version 2 when confronted with exercise result" in {
      val hasExerciseResult =
        dummyExerciseTransaction map3 (identity, identity, v => ValueOptional(Some(v)))
      assignTxVersion(assignValueVersions(hasExerciseResult)) shouldBe Right(
        maxVersion(supportedTxVersions.min, TransactionVersion("2")))
    }

    "crash the picked version is more recent that the maximal supported version" in {
      val supportedVersions = VersionRange(TransactionVersion("1"), TransactionVersion("5"))
      val hasExerciseResult =
        dummyExerciseWithResultTransaction map3 (identity, identity, v => ValueOptional(Some(v)))
      TransactionVersions.assignVersion(assignValueVersions(hasExerciseResult), supportedVersions) shouldBe 'left
    }

  }

  private[this] def assignValueVersions[Nid, Cid <: ContractId](
      t: GenTransaction[Nid, Cid, Value[Cid]],
  ): GenTransaction[Nid, Cid, VersionedValue[Cid]] =
    t map3 (identity, identity, ValueVersions.assertAsVersionedValue(_, supportedValVersions))

  private[this] def assignTxVersion[Nid, Cid <: ContractId](
      t: GenTransaction[Nid, Cid, VersionedValue[Cid]],
  ): Either[String, TransactionVersion] =
    TransactionVersions.assignVersion(t, supportedTxVersions)

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
