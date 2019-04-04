// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import java.time.Duration

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.ledger.api.v1.commands.Commands

class CommandUpdater(timeProviderO: Option[TimeProvider], ttl: Duration, overrideTtl: Boolean) {

  def applyOverrides(commands: Commands): Commands = {
    timeProviderO.fold(overrideWithoutLetChange(commands))(overrideWithLetChange(commands, _))

  }

  private def overrideWithoutLetChange(commands: Commands) = {
    if (overrideTtl) {
      commands.copy(maximumRecordTime = commands.ledgerEffectiveTime.map(l =>
        fromInstant(toInstant(l).plus(ttl))))
    } else commands
  }

  private def overrideWithLetChange(commands: Commands, timeProvider: TimeProvider) = {
    val ledgerEffectiveTime = timeProvider.getCurrentTime
    val oldMrt = commands.maximumRecordTime
    commands.copy(
      ledgerEffectiveTime = Some(fromInstant(ledgerEffectiveTime)),
      maximumRecordTime = oldMrt.fold {
        Some(fromInstant(ledgerEffectiveTime.plus(ttl)))
      } { prevMrt =>
        if (overrideTtl) {
          Some(fromInstant(ledgerEffectiveTime.plus(ttl)))
        } else {
          val prevTtl =
            Duration.between(toInstant(commands.getLedgerEffectiveTime), toInstant(prevMrt))
          Some(fromInstant(ledgerEffectiveTime.plus(prevTtl)))
        }
      }
    )
  }
}
