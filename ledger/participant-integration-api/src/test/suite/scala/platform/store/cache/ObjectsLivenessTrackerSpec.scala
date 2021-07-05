// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.atomic.AtomicLong

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ObjectsLivenessTrackerSpec extends AnyFlatSpec with Matchers {
  behavior of classOf[ObjectsLivenessTracker[_]].getSimpleName

  it should "track" in {
    val counter = new AtomicLong(0L)
    val tracker = ObjectsLivenessTracker[Object](
      { () => counter.incrementAndGet(); () },
      { () => counter.decrementAndGet(); () },
    )

    var daRefs = (1 to 1000).map(_ => new Object)
    daRefs.foreach(tracker.track)

    counter.get() shouldBe 1000L
    daRefs = null

    System.gc()

    Thread.sleep(1000L)
    tracker.refs.isEmpty shouldBe true
    counter.get() shouldBe 0L
  }
}
