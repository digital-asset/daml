// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.value.Value
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ActiveContractKeysPoolSpec extends AnyFlatSpec with Matchers {

  it should "put and pop from a pool" in {
    val tested = new ActiveContractKeysPool(RandomnessProvider.forSeed(0))
    intercept[NoSuchElementException](tested.getAndRemoveContractKey(templateName = "t1"))
    tested.addContractKey(templateName = "t1", key = makeValue("1"))
    intercept[NoSuchElementException](tested.getAndRemoveContractKey(templateName = "t2"))
    tested.getAndRemoveContractKey("t1") shouldBe makeValue("1")
    intercept[IndexOutOfBoundsException](tested.getAndRemoveContractKey(templateName = "t1"))
    tested.addContractKey(templateName = "t1", key = makeValue("1"))
    tested.addContractKey(templateName = "t1", key = makeValue("2"))
    tested.addContractKey(templateName = "t1", key = makeValue("3"))
    tested.addContractKey(templateName = "t2", key = makeValue("1"))
    tested.getAndRemoveContractKey("t1") shouldBe makeValue("3")
    tested.getAndRemoveContractKey("t1") shouldBe makeValue("1")
    tested.getAndRemoveContractKey("t2") shouldBe makeValue("1")
  }

  private def makeValue(payload: String): Value = Value(Value.Sum.Text(payload))
}
