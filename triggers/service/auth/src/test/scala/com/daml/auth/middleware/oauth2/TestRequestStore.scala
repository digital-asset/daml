// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import com.daml.auth.middleware.api.RequestStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class TestRequestStore extends AsyncWordSpec with Matchers {
  "return None on missing element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    store.pop(0) should ===(None)
  }

  "return previously put element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    store.put(0, "zero")
    store.pop(0) should ===(Some("zero"))
  }

  "return None on previously popped element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    store.put(0, "zero")
    store.pop(0)
    store.pop(0) should ===(None)
  }

  "store multiple elements" in {
    val store = new RequestStore[Int, String](3, 1.day)
    store.put(0, "zero")
    store.put(1, "one")
    store.put(2, "two")
    store.pop(0) should ===(Some("zero"))
    store.pop(1) should ===(Some("one"))
    store.pop(2) should ===(Some("two"))
  }

  "store no more than max capacity" in {
    val store = new RequestStore[Int, String](2, 1.day)
    assert(store.put(0, "zero"))
    assert(store.put(1, "one"))
    assert(!store.put(2, "two"))
    store.pop(0) should ===(Some("zero"))
    store.pop(1) should ===(Some("one"))
    store.pop(2) should ===(None)
  }

  "return None on timed out element" in {
    var time: Long = 0
    val store = new RequestStore[Int, String](1, 1.day, () => time)
    store.put(0, "zero")
    time += 1.day.toNanos
    store.pop(0) should ===(None)
  }

  "free capacity for timed out elements" in {
    var time: Long = 0
    val store = new RequestStore[Int, String](1, 1.day, () => time)
    assert(store.put(0, "zero"))
    assert(!store.put(1, "one"))
    time += 1.day.toNanos
    assert(store.put(2, "two"))
    store.pop(2) should ===(Some("two"))
  }
}
