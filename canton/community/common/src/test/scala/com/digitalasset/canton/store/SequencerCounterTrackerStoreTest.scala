// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.lifecycle.HasCloseContext
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpecLike

trait SequencerCounterTrackerStoreTest extends CursorPreheadStoreTest {
  this: AsyncWordSpecLike with BaseTest with HasCloseContext =>

  def sequencerCounterTrackerStore(mk: () => SequencerCounterTrackerStore): Unit = {
    "sequencer counter tracker store" should {
      behave like cursorPreheadStore(() => mk().cursorStore, SequencerCounter.apply)
    }
  }
}
