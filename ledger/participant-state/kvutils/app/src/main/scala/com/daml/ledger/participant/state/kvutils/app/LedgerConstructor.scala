// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.ParticipantId
import scopt.OptionParser

trait LedgerConstructor[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def apply(participantId: ParticipantId, config: ExtraConfig)(
      implicit materializer: Materializer,
  ): KeyValueLedger
}

object LedgerConstructor {
  def apply(construct: ParticipantId => KeyValueLedger): LedgerConstructor[Unit] =
    new LedgerConstructor[Unit] {
      override val defaultExtraConfig: Unit = ()

      override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
        ()

      override def apply(participantId: ParticipantId, config: Unit)(
          implicit materializer: Materializer,
      ): KeyValueLedger =
        construct(participantId)
    }
}
