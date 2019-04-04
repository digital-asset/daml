package com.daml.ledger.participant.state.v1.impl.reference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{ReadService, UpdateId}

class ReferenceReadService(ledger: Ledger) extends ReadService {

  override def stateUpdates(beginAfter: Option[UpdateId]): Source[(UpdateId, v1.Update), NotUsed] =
    ledger.ledgerSyncEvents(beginAfter)

}
