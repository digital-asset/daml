// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.Future

class SequencerMemberCacheTest extends AsyncWordSpec with BaseTest {
  val testRunId = new AtomicInteger()
  "member cache" should {
    "only cache lookups when the member exists" in {
      val lookupMemberCallCount = new AtomicInteger(0)
      val registeredMemberRef = new AtomicReference[Option[RegisteredMember]](None)
      val cache = new SequencerMemberCache(_ =>
        Future.successful {
          lookupMemberCallCount.incrementAndGet()
          registeredMemberRef.get()
        }
      )
      val registeredTs = CantonTimestamp.now()
      val registeredMember = RegisteredMember(SequencerMemberId(42), registeredTs)

      for {
        firstResult <- cache(participant1)
        _ = firstResult shouldBe None
        _ = registeredMemberRef.set(Some(registeredMember))
        secondResult <- cache(participant1)
        _ = secondResult.value shouldBe registeredMember
        thirdResult <- cache(participant1)
        _ = secondResult shouldBe thirdResult
      } yield {
        // first call when there was no result, and then one more for the first successful lookup
        // the final lookup should be served from the cache
        lookupMemberCallCount.get() shouldBe 2
      }
    }
  }
}
