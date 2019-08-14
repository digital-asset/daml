// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.time

import java.time.Duration

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.client.services.testing.time.StaticTime

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
