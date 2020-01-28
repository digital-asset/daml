// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

trait LedgerFactory[T <: KeyValueLedger, ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def owner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      config: ExtraConfig,
  )(implicit materializer: Materializer): ResourceOwner[T]
}

object LedgerFactory {
  def apply[T <: KeyValueLedger](
      newOwner: (LedgerId, ParticipantId) => ResourceOwner[T],
  ): LedgerFactory[T, Unit] =
    new SimpleLedgerFactory(newOwner)

  class SimpleLedgerFactory[T <: KeyValueLedger](
      newOwner: (LedgerId, ParticipantId) => ResourceOwner[T]
  ) extends LedgerFactory[T, Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()

    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: Unit,
    )(implicit materializer: Materializer): ResourceOwner[T] =
      newOwner(ledgerId, participantId)
  }
}
