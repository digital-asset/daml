// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.time.Duration
import java.util.UUID

import com.daml.lf.data.Ref.Identifier
import com.daml.platform.services.time.TimeProviderType

import scala.concurrent.duration.FiniteDuration

package trigger {

  case class LedgerConfig(
      host: String,
      port: Int,
      timeProvider: TimeProviderType,
      commandTtl: Duration,
      maxInboundMessageSize: Int,
  )

  case class TriggerRestartConfig(
      minRestartInterval: FiniteDuration,
      maxRestartInterval: FiniteDuration,
      restartIntervalRandomFactor: Double = 0.2,
  )

  final case class SecretKey(value: String)
  final case class UserCredentials(token: EncryptedToken)

  final case class RunningTrigger(
      triggerInstance: UUID,
      triggerName: Identifier,
      credentials: UserCredentials,
      // TODO(SF, 2020-0610): Add access token field here in the
      // presence of authentication.
  )
}
