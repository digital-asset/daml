// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import data.ImmArray
import value.{Value, ValueVersion, ValueVersions}
import Value.{ContractId, ValueOptional, VersionedValue}
import com.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.HashMap

class TransactionVersionSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {
  import TransactionVersionSpec._

  import VersionTimeline.maxVersion

  private[this] val supportedValVersions =
    ValueVersions.SupportedDevVersions.copy(min = ValueVersion("1"))
  private[this] val supportedTxVersions =
    TransactionVersions.SupportedDevOutputVersions.copy(min = TransactionVersion("1"))

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

  "TransactionVersions.assignVersions" should {

    import VersionTimeline.Implicits._

    val Seq(v1_1, v1_5, v1_6, v1_7) = Seq("1", "5", "6", "7").map(minor =>
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable(minor)))

    val v1_dev =
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Dev)

    val langVersions = Table("language version", v1_1, v1_5, v1_6, v1_7)

    "pick always the min supported version for package using LF 1.7 or earlier" in {
      val supportedVersionRanges =
        Table(
          "supported Versions",
          VersionRange(TransactionVersion("10"), TransactionVersion("10")),
          VersionRange(TransactionVersion("10"), TransactionVersion("11")),
          VersionRange(TransactionVersion("11"), TransactionVersion("11")),
        )

      forEvery(supportedVersionRanges) { supportedTxVersions =>
        val expectedTxVersion = supportedTxVersions.min
        val expectedOutput = Right(expectedTxVersion)

        TransactionVersions.assignVersions(supportedTxVersions, Seq.empty) shouldBe expectedOutput
        forEvery(langVersions) { langVersion =>
          TransactionVersions.assignVersions(supportedTxVersions, Seq(langVersion)) shouldBe expectedOutput
        }
      }
    }

    "pick version 11 for package using LF 1.dev" in {
      val supportedVersionRanges =
        Table(
          "supported Versions",
          VersionRange(TransactionVersion("10"), TransactionVersion("11")),
          VersionRange(TransactionVersion("11"), TransactionVersion("11")),
        )

      forEvery(supportedVersionRanges) { supportedTxVersions =>
        val expectedTxVersion = TransactionVersion("11")
        val expectedOutput = Right(expectedTxVersion)

        TransactionVersions.assignVersions(supportedTxVersions, Seq(v1_dev)) shouldBe expectedOutput
        forEvery(langVersions) { langVersion =>
          TransactionVersions.assignVersions(supportedTxVersions, Seq(langVersion, v1_dev)) shouldBe expectedOutput
        }
      }
    }

    "fail if the inferred version is not supported" in {
      forEvery(langVersions) { langVersion =>
        TransactionVersions.assignVersions(
          VersionRange(TransactionVersion("9"), TransactionVersion("9")),
          Seq(langVersion)) shouldBe 'left
        TransactionVersions.assignVersions(
          VersionRange(TransactionVersion("10"), TransactionVersion("10")),
          Seq(v1_dev, langVersion)) shouldBe 'left
      }
    }

  }

  "TransactionVersions.assignValueVersion" should {
    "be stable" in {

      val testCases = Table(
        "input" -> "output",
        TransactionVersion("10") -> ValueVersion("6"),
        TransactionVersion("11") -> ValueVersion("7")
      )

      forEvery(testCases) { (input, expectedOutput) =>
        TransactionVersions.assignValueVersion(input) shouldBe expectedOutput
      }

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

  private[this] val singleId = NodeId(0)
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
