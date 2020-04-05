// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.time

import java.time.Duration

import com.daml.api.util.TimeProvider
import com.daml.ledger.client.services.testing.time.StaticTime

import scala.concurrent.ExecutionContext

case class TimeProviderWithType(time: TimeProvider, `type`: TimeProviderType)

object TimeProviderFactory {

  def apply(timeProviderType: TimeProviderType, ledgerTime: Option[StaticTime])(
      implicit ec: ExecutionContext): Option[TimeProviderWithType] =
    timeProviderType match {
      case TimeProviderType.Auto =>
        ledgerTime.fold(
          Some(TimeProviderWithType(TimeProvider.UTC, TimeProviderType.WallClock))
        )(
          t => Some(TimeProviderWithType(t, TimeProviderType.Static))
        )
      case TimeProviderType.WallClock =>
        Some(TimeProviderWithType(TimeProvider.UTC, TimeProviderType.WallClock))
      case TimeProviderType.Static =>
        ledgerTime.map(t => TimeProviderWithType(t, TimeProviderType.Static))
      case TimeProviderType.Simulated =>
        ledgerTime.map(lt => {
          val utc: TimeProvider = TimeProvider.UTC
          val diff = Duration.between(lt.getCurrentTime, utc.getCurrentTime)
          TimeProviderWithType(
            TimeProvider.MappedTimeProvider(utc, i => i minus diff),
            TimeProviderType.Simulated)
        })
    }
}
