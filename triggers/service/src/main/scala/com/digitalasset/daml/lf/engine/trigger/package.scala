// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.time.Duration
import java.util.UUID

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.data.Ref.Identifier
import com.daml.platform.services.time.TimeProviderType

import akka.http.scaladsl.model.Uri
import scala.concurrent.duration.FiniteDuration

package trigger {

  sealed trait AuthConfig
  case object NoAuth extends AuthConfig
  final case class AuthMiddleware(internal: Uri, external: Uri) extends AuthConfig

  import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
  import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
  import com.daml.logging.LoggingContextOf

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
      triggerParty: Party,
      triggerApplicationId: ApplicationId,
      triggerAccessToken: Option[AccessToken],
      triggerRefreshToken: Option[RefreshToken],
      triggerReadAs: Set[Party],
  ) {
    private[trigger] def withLoggingContext[T]: (LoggingContextOf[Trigger] => T) => T =
      Trigger.newLoggingContext(
        triggerName,
        triggerParty,
        triggerReadAs,
        triggerInstance.toString,
        triggerApplicationId,
      )
  }
}
