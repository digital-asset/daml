package com.daml.ledger.participant.state.v1

import akka.NotUsed
import akka.stream.scaladsl.Source

trait ReadService {
  def stateUpdates(
      beginAfter: Option[UpdateId]): Source[(UpdateId, Update), NotUsed]
}
