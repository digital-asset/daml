// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

final class StateServiceIT extends LedgerTestSuite {
  test(
    "StateServiceGetConnectedSynchronizersWithoutParty",
    "Get connected synchronizers without party filter",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      connectedSynchronizers <- ledger.getConnectedSynchronizers(None, None)
    } yield {
      assert(connectedSynchronizers.sizeIs > 0, "Expected connected synchronizers")
    }
  })
}
