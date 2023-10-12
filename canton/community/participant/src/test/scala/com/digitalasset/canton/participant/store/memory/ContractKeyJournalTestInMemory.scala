// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.ContractKeyJournalTest
import com.digitalasset.canton.{BaseTest, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpec

class ContractKeyJournalTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with ContractKeyJournalTest
    with TestMetrics {

  "InMemoryContractKeyJournal" should {
    behave like contractKeyJournal(ec => new InMemoryContractKeyJournal(loggerFactory)(ec))
  }
}
