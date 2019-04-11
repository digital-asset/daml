// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{ReadService, Offset}

// FIXME (SM): why not implement that interface directly on 'Ledger'?
class ReferenceReadService(ledger: Ledger) extends ReadService {

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, v1.Update), NotUsed] =
    ledger.ledgerSyncEvents(beginAfter)

}
