// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SizeCappedMapTest extends AnyWordSpec with BeforeAndAfterEach with Matchers {

  private var sut: SizeCappedMap[Long, Long] = _

  override protected def beforeEach(): Unit = {
    sut = new SizeCappedMap[Long, Long](1, 2)
  }

  "ExpiringMap" when {

    "has spare capacity " should {

      "retain elements as a new one is inserted" in {

        sut.put(0L, 0L)
        sut.put(1L, 1L)
        Option(sut.get(0L)) should not be empty
        Option(sut.get(1L)) should not be empty
      }
    }

    "does not have spare capacity" should {

      "drop the oldest element as a new one is inserted" in {

        sut.put(0L, 0L)
        sut.put(1L, 1L)
        sut.put(2L, 2L)
        Option(sut.get(0L)) shouldBe empty
        Option(sut.get(1L)) should not be empty
        Option(sut.get(2L)) should not be empty
      }
    }
  }

}
