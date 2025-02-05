// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.InMemoryOutputMetadataStore
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryOutputMetadataStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with OutputMetadataStoreTest {

  "InMemoryOutputBlockMetadataStore" should {
    behave like outputBlockMetadataStore(() => new InMemoryOutputMetadataStore(loggerFactory))
  }
}
