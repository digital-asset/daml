// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.UUID

import akka.actor.typed.ActorRef
import akka.http.scaladsl.Http.ServerBinding

sealed trait Message

final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message

final case class StartFailed(cause: Throwable) extends Message

final case class Started(binding: ServerBinding) extends Message

case object Stop extends Message

// Messages passed to the server from a TriggerRunnerImpl

final case class TriggerStarting(triggerInstance: UUID) extends Message

final case class TriggerStarted(triggerInstance: UUID) extends Message

final case class TriggerInitializationFailure(
    triggerInstance: UUID,
    cause: String
) extends Message

final case class TriggerRuntimeFailure(
    triggerInstance: UUID,
    cause: String
) extends Message
