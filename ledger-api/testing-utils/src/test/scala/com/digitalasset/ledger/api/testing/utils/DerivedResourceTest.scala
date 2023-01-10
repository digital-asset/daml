// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class DerivedResourceTest extends AnyWordSpec with BeforeAndAfterEach with Matchers {

  private var base: Resource[Int] = _
  private var derived: Resource[(Int, Long)] = _

  private var baseTornDown = false
  private var derivedTornDown = false

  override def beforeEach(): Unit = {
    base = new ManagedResource[Int] {
      override protected def construct(): Int = 1

      override protected def destruct(resource: Int): Unit = baseTornDown = true
    }

    derived = new DerivedResource[Int, Long](base) {
      override protected def construct(source: Int): Long = source.toLong

      override protected def destruct(target: Long): Unit = derivedTornDown = true
    }

    baseTornDown = false
  }

  "Derived and base resources" when {

    "uninitialized" should {

      "throw error when base is accessed" in {
        assertThrows[IllegalStateException](base.value)
      }

      "throw error when derived is accessed" in {
        assertThrows[IllegalStateException](derived.value)
      }
    }

    "initialized" should {

      "allow access to base" in {

        derived.setup()
        base.value shouldEqual 1
      }

      "allow access to derived" in {

        derived.setup()
        derived.value shouldEqual (1 -> 1L)
      }
    }

    "torn down" should {

      "execute destructor method in base" in {
        derived.setup()
        derived.close()
        baseTornDown shouldEqual true
      }

      "execute destructor method in derived" in {
        derived.setup()
        derived.close()
        derivedTornDown shouldEqual true
      }

      "throw error when base is accessed" in {
        derived.setup()
        derived.close()
        assertThrows[IllegalStateException](base.value)
      }

      "throw error when derived is accessed" in {
        derived.setup()
        derived.close()
        assertThrows[IllegalStateException](derived.value)
      }
    }
  }
}
