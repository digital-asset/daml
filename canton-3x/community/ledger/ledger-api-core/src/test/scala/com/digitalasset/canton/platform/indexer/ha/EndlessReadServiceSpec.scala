// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class EndlessReadServiceSpec
    extends AnyFlatSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  behavior of EndlessReadService.getClass.getSimpleName

  it should "correctly compute index from offset" in forAll { (index: Int) =>
    whenever(index > 0) {
      val offset = EndlessReadService.offset(index)
      val index2 = EndlessReadService.index(offset)
      index2 shouldBe index
    }
  }

  it should "use deterministic times" in forAll { (index: Int) =>
    whenever(index > 0) {
      val t1 = EndlessReadService.recordTime(index)
      val t2 = EndlessReadService.recordTime(index)
      t2 shouldBe t1
    }
  }

  it should "use deterministic contract ids" in forAll { (index: Int) =>
    whenever(index > 0) {
      val c1 = EndlessReadService.cid(index)
      val c2 = EndlessReadService.cid(index)
      c2 shouldBe c1
    }
  }
}
