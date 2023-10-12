// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import org.scalatest.wordspec.AnyWordSpec

class ActivenessCheckTest extends AnyWordSpec with BaseTest {

  import ConflictDetectionHelpers.*

  val coid00: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  val coid01: LfContractId = ExampleTransactionFactory.suffixedId(0, 1)
  val coid10: LfContractId = ExampleTransactionFactory.suffixedId(1, 0)

  "ActivenessCheck" should {

    val set1 = Set(coid00, coid01)
    val set2 = Set(coid00, coid10)

    "not allow overlapping sets" in {
      assertThrows[IllegalArgumentException](
        mkActivenessCheck[LfContractId](fresh = set1, free = set2)
      )
      assertThrows[IllegalArgumentException](mkActivenessCheck(fresh = set1, active = set2))
      assertThrows[IllegalArgumentException](mkActivenessCheck(free = set1, active = set2))
    }

    "not allow prior states without checking activeness" in {
      assertThrows[IllegalArgumentException](
        mkActivenessCheck[LfContractId](fresh = set1, prior = set2)
      )
    }
  }
}
