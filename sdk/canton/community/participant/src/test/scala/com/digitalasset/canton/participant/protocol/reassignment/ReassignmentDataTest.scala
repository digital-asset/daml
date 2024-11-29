// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ReassignmentDataTest extends AnyWordSpec with Matchers with EitherValues {
  private implicit def toOffset(i: Long): Offset = Offset.tryFromLong(i)

  "ReassignmentData.ReassignmentGlobalOffset" should {
    val unassignment1 = UnassignmentGlobalOffset(1)
    val unassignment2 = UnassignmentGlobalOffset(2)

    val assignment1 = AssignmentGlobalOffset(1)
    val assignment2 = AssignmentGlobalOffset(2)

    val reassignment12 = ReassignmentGlobalOffsets.create(1, 2).value
    val reassignment21 = ReassignmentGlobalOffsets.create(2, 1).value

    "be mergeable (UnassignmentGlobalOffset)" in {
      unassignment1.merge(unassignment1).value shouldBe unassignment1
      unassignment1.merge(unassignment2) shouldBe Symbol("left") // 1 != 2

      unassignment1.merge(assignment1) shouldBe Symbol("left") // 1 == 1
      unassignment1.merge(assignment2).value shouldBe reassignment12

      unassignment1.merge(reassignment12).value shouldBe reassignment12
      unassignment1.merge(reassignment21) shouldBe Symbol("left") // 1 != 2
    }

    "be mergeable (AssignmentGlobalOffset)" in {
      assignment1.merge(unassignment1) shouldBe Symbol("left")
      assignment1.merge(unassignment2).value shouldBe reassignment21

      assignment1.merge(assignment1).value shouldBe assignment1
      assignment1.merge(assignment2) shouldBe Symbol("left")

      assignment1.merge(reassignment12) shouldBe Symbol("left")
      assignment1.merge(reassignment21).value shouldBe reassignment21
    }

    "be mergeable (ReassignmentGlobalOffsets)" in {
      reassignment12.merge(unassignment1).value shouldBe reassignment12
      reassignment12.merge(unassignment2) shouldBe Symbol("left")

      reassignment12.merge(assignment1) shouldBe Symbol("left")
      reassignment12.merge(assignment2).value shouldBe reassignment12

      reassignment12.merge(reassignment12).value shouldBe reassignment12
      reassignment12.merge(reassignment21) shouldBe Symbol("left")
    }
  }
}
