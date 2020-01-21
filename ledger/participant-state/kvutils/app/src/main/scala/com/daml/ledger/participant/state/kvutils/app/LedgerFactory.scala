// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import scopt.OptionParser

trait LedgerFactory[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def apply(ledgerId: LedgerId, participantId: ParticipantId, config: ExtraConfig)(
      implicit materializer: Materializer,
  ): KeyValueLedger
}

object LedgerFactory {
  def apply(construct: (LedgerId, ParticipantId) => KeyValueLedger): LedgerFactory[Unit] =
    new SimpleLedgerFactory(construct)

  class SimpleLedgerFactory(construct: (LedgerId, ParticipantId) => KeyValueLedger)
      extends LedgerFactory[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()

    override def apply(ledgerId: LedgerId, participantId: ParticipantId, config: Unit)(
        implicit materializer: Materializer,
    ): KeyValueLedger =
      construct(ledgerId, participantId)
  }
}
