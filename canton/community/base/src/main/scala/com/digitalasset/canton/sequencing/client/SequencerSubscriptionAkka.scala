// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.Done
import akka.stream.KillSwitch
import akka.stream.scaladsl.Source
import com.digitalasset.canton.health.HealthComponent
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.util.AkkaUtil.WithKillSwitch

import scala.concurrent.Future

/** Wrapper for an Akka source delivering the stream of sequenced events.
  * The [[akka.stream.KillSwitch]] can be used to terminate the stream.
  * The materialized [[scala.concurrent.Future]] completes
  * after the internal processing in the source has finished
  * after having been closed through the [[akka.stream.KillSwitch]].
  */
final case class SequencerSubscriptionAkka[+E](
    source: Source[WithKillSwitch[Either[E, OrdinarySerializedEvent]], (KillSwitch, Future[Done])],
    health: HealthComponent,
)
