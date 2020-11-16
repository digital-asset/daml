// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.time.Duration
import java.util.UUID

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.data.Ref.Identifier
import com.daml.platform.services.time.TimeProviderType

import akka.http.scaladsl.model.Uri
import scala.concurrent.duration.FiniteDuration
import scalaz.{@@, Tag}

package trigger {

  sealed trait AuthConfig
  case object NoAuth extends AuthConfig
  final case class AuthMiddleware(uri: Uri) extends AuthConfig

  object Tagged {
    sealed trait AccessTokenTag
    type AccessToken = String @@ AccessTokenTag
    val AccessToken = Tag.of[AccessTokenTag]
  }
  import Tagged._
  import com.daml.ledger.api.refinements.ApiTypes.ApplicationId

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
      triggerToken: Option[AccessToken],
  )
}
