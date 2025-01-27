// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.InMemoryOutputBlockMetadataStore
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryOutputBlockMetadataStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with OutputBlockMetadataStoreTest {

  "InMemoryOutputBlockMetadataStore" should {
    behave like outputBlockMetadataStore(() => new InMemoryOutputBlockMetadataStore)
  }
}
