// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TagsTest extends AnyWordSpec with Matchers {

  "ReassignmentId" when {
    val sync1 = SynchronizerId.tryFromString("sync1::namespace")
    val sync2 = SynchronizerId.tryFromString("sync2::namespace")
    val ts = CantonTimestamp.Epoch
    val cid1 = LfContractId.assertFromString("00" + "00" * 32 + "01")
    val cid2 = LfContractId.assertFromString("00" + "00" * 32 + "02")
    val rac1 = ReassignmentCounter(1)
    val rac2 = ReassignmentCounter(2)

    val examples = for {
      source <- Seq(sync1, sync2)
      target <- Seq(sync1, sync2)
      timestamp <- Seq(ts, ts.plusSeconds(1))
      cidCtrs <- Seq(Seq.empty, Seq(cid1 -> rac1, cid2 -> rac2))
    } yield (Source(source), Target(target), timestamp, cidCtrs)

    val mk = ReassignmentId.apply.tupled

    "different args produce different ids" should {
      examples.zipWithIndex.foreach { case (a, i) =>
        examples.zipWithIndex.foreach { case (b, j) =>
          if (i == j) () // skip comparing the same arguments
          else withClue(s"comparing args: $a with $b")(mk(a) should not equal mk(b))
        }
      }
    }

    "equivalent args produce the same id" should {
      val source = Source(sync1)
      val target = Target(sync2)
      "ignore order of contract id counters" in {
        val oneThenTwo = ReassignmentId(source, target, ts, Seq(cid1 -> rac1, cid2 -> rac2))
        val twoThenOne = ReassignmentId(source, target, ts, Seq(cid2 -> rac2, cid1 -> rac1))
        oneThenTwo should equal(twoThenOne)
      }
    }

    "fromBytes of toBytes" should {
      "roundtrip" in {
        examples.map(mk).foreach { id =>
          ReassignmentId.fromBytes(id.toBytes) shouldBe Right(id)
        }
      }
    }

    "fromProtoPrimitive of toProtoPrimitive" should {
      "roundtrip" in {
        examples.map(mk).foreach { id =>
          ReassignmentId.fromProtoPrimitive(id.toProtoPrimitive) shouldBe Right(id)
        }
      }
    }

    "create" should {
      "fail if empty" in {
        ReassignmentId.create("") shouldBe Left(
          "invalid ReassignmentId(): no ReassignmentId version"
        )
      }
      "fail if invalid version prefix" in {
        ReassignmentId.create("FFFF") shouldBe Left(
          "invalid ReassignmentId(FFFF): cannot parse ReassignmentId bytes: invalid version: -1"
        )
      }
      "fail if invalid hex format" in {
        ReassignmentId.create("this ain't hex") shouldBe Left(
          "invalid ReassignmentId(this ain't hex): invalid hex"
        )
      }
      "succeed for valid values" in {
        ReassignmentId.create("0001").toOption isDefined
      }
    }

    "toString" should {
      "be sensible" in {
        ReassignmentId.create("0001").map(_.toString) shouldBe Right("ReassignmentId(0001)")
      }
    }
  }
}
