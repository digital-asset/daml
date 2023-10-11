// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.NonNegativeDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final case class TestCase(id: Int, name: String)

class ConsoleMacrosTest extends AnyWordSpec with BaseTest {
  "ConsoleMacrosTest" should {
    val expected = Set("id:Int", "name:String")
    "object_args" in {
      val tmp = TestCase(1, "one")
      val res = ConsoleMacros.utils.object_args(tmp)
      assertResult(2, tmp)(res.size)
      assertResult(expected)(res.toSet)
    }

    "type_args" in {
      val res = ConsoleMacros.utils.type_args[TestCase]
      assertResult(2, res)(res.size)
      assertResult(expected)(res.toSet)
    }

    "retry_until_true should back off" in {
      val started = Instant.now()
      val counter = new AtomicInteger(0)
      try {
        ConsoleMacros.utils
          .retry_until_true(timeout = NonNegativeDuration.tryFromDuration(1.second)) {
            counter.incrementAndGet()
            false
          }
        fail("should have bounced")
      } catch {
        case _: IllegalStateException =>
      }
      val ended = Instant.now()
      (ended.toEpochMilli - started.toEpochMilli) should be > 900L

      // Upper bound derivation:
      // - There is 1 invocation up front.
      // - There is 1 invocation after sleeping for 2^0 .. 2^9 milliseconds.
      // - Total time slept is 2^0 + ... + 2^9 = 1^10 - 1 = 1023 milliseconds. So the time out must have elapsed.
      // - Total number of invocations is 1 + 10 = 11.
      // - Sometimes, it might be 12, as the deadline stuff is working on the nanosecond level
      counter.get() should be <= 12
    }

  }
}
