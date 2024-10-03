// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class ReassignmentStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ReassignmentStoreTest {

  private def mk(domain: IndexedDomain): InMemoryReassignmentStore =
    new InMemoryReassignmentStore(TargetDomainId(domain.domainId), loggerFactory)

  "ReassignmentStoreTestInMemory" should {
    behave like reassignmentStore(mk)
  }
}
