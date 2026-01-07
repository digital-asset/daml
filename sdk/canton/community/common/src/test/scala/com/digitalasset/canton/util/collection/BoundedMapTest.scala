// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class BoundedMapTest extends AnyWordSpec with BaseTest {
  "BoundedMap" should {
    "enqueue messages one by one and drop the oldest message" in {
      val evict = mock[(String, String) => Unit]
      val map = BoundedMap[String, String](2, evict)

      map.put("key1", "value1")
      map.put("key2", "value2")
      map.toList should contain theSameElementsInOrderAs Seq(("key1", "value1"), ("key2", "value2"))
      verifyZeroInteractions(evict)

      map.put("key3", "value3")
      map.toList should contain theSameElementsInOrderAs Seq(("key2", "value2"), ("key3", "value3"))
      verify(evict, times(1)).apply("key1", "value1")
    }
  }
}
