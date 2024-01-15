// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.time.Duration
import java.util.UUID

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
import com.daml.platform.services.time.TimeProviderType

import org.apache.pekko.http.scaladsl.model.Uri
import scala.concurrent.duration.FiniteDuration

package trigger {

  sealed trait AuthConfig
  case object NoAuth extends AuthConfig
  final case class AuthMiddleware(internal: Uri, external: Uri) extends AuthConfig

  import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}

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

  final case class RunningTrigger(
      triggerInstance: UUID,
      triggerName: Identifier,
      triggerParty: Ref.Party,
      triggerApplicationId: Option[Ref.ApplicationId],
      triggerAccessToken: Option[AccessToken],
      triggerRefreshToken: Option[RefreshToken],
      triggerReadAs: Set[Ref.Party],
  ) {
    private[trigger] def withTriggerLogContext[T]: (TriggerLogContext => T) => T =
      Trigger.newTriggerLogContext(
        triggerName,
        triggerParty,
        triggerReadAs,
        triggerInstance.toString,
        triggerApplicationId,
      )
  }
}
