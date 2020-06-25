package com.daml.lf.engine

import java.time.Duration
import java.util.UUID

import akka.actor.typed.ActorRef
import com.daml.lf.data.Ref.Identifier
import com.daml.platform.services.time.TimeProviderType

package object trigger {

  case class LedgerConfig(
      host: String,
      port: Int,
      timeProvider: TimeProviderType,
      commandTtl: Duration,
  )

  case class TriggerRunnerConfig(
      maxInboundMessageSize: Int,
      maxFailureNumberOfRetries: Int,
      failureRetryTimeRange: Duration
  )

  final case class SecretKey(value: String)
  final case class UserCredentials(token: EncryptedToken)

  final case class RunningTrigger(
      triggerInstance: UUID,
      triggerName: Identifier,
      credentials: UserCredentials,
      // TODO(SF, 2020-0610): Add access token field here in the
      // presence of authentication.
      runner: ActorRef[TriggerRunner.Message]
  )
}
