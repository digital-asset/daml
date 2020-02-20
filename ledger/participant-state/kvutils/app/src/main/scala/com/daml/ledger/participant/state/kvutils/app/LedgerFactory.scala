// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

import scala.concurrent.ExecutionContext

trait LedgerFactory[T <: KeyValueLedger, ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      config: ExtraConfig,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): ResourceOwner[T]
}

object LedgerFactory {

  abstract class SimpleLedgerFactory[T <: KeyValueLedger] extends LedgerFactory[T, Unit] {
    override final val defaultExtraConfig: Unit = ()

    override final def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()
  }
}
