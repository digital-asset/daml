// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.Duration

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Configuration, LedgerId, Offset, TimeModel}
import com.daml.ledger.api.health.ReportsHealth

/**
  * Defines how a participant's state is read from the ledger.
  *
  * For a detailed description of the required semantics of state updates see
  * [[com.daml.ledger.participant.state.v1.ReadService]].
  */
trait LedgerReader extends ReportsHealth {

  /**
    * Streams raw updates from the given offset for the participant.
    *
    * In case an offset is not specified, all updates must be streamed from the oldest known state.
    * Each update is defined either as an opaque log entry ID and an
    * envelope ([[com.daml.ledger.participant.state.kvutils.api.LedgerRecord]]).
    *
    * @param startExclusive offset right after which updates must be streamed; in case not specified updates
    *                       must be returned from the beginning
    * @return stream of updates
    */
  def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed]

  /**
    * Get the ledger's ID from which this reader instance streams events.
    * Should not be a blocking operation.
    *
    * @return ID of the ledger from which this reader streams events
    */
  def ledgerId(): LedgerId
}

object LedgerReader {

  /**
    * Default initial ledger configuration used by [[KeyValueParticipantStateReader]].
    */
  val DefaultConfiguration: Configuration = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )
}
